-- The 'EffectHandler' type synonym in effectful-core expands to a
-- constraint that GHC's redundant-constraint check flags on the
-- handler's signature, even though the constraint is required for
-- 'interpret' to type-check. Suppress the warning at the file level.
{-# OPTIONS_GHC -Wno-redundant-constraints #-}

{- | OpenTelemetry-traced interpreter for the 'KafkaConsumer' effect.

Drop-in alternative to 'Kafka.Effectful.Consumer.Interpreter.runKafkaConsumer'
that opens a Consumer-kind span on every successful record return from
'pollMessage' \/ 'pollMessageBatch', rooted at the W3C trace context
extracted from the record\'s Kafka headers (or as a new root span when
no inbound context is present). Polls that return @Nothing@ on timeout
do not open a span, preserving the existing timeout-returns-@Nothing@
semantics.

Non-polling operations (offset commit, partition assignment, etc.) are
passed through unchanged.

Each record\'s context is installed only for the duration of its own span and
is then detached, so records never chain onto one another: a record with no
inbound context starts a new root even when the record before it on the same
thread carried a remote one. That per-record isolation is what makes the
\"new root span when no inbound context is present\" promise above true in
practice.

Note that the span covers only the act of receiving the record, not the
application\'s processing of it — it is effectively a zero-duration marker at
the point of delivery. Covering processing would require a handler-wrapping
API, which is deliberately out of scope for this module.

The design parallels the upstream
@hs-opentelemetry-instrumentation-hw-kafka-client@\'s
@OpenTelemetry.Instrumentation.Kafka.pollMessage@.

@since 0.2.0.0
-}
module Kafka.Effectful.OpenTelemetry.Consumer.Interpreter (
    -- * Interpreter
    runKafkaConsumerTraced,

    -- * Internal — exported for tests
    withConsumerSpan,
)
where

import Control.Monad (void)
import Data.ByteString (ByteString)
import Data.Foldable (for_)
import Effectful (Eff, IOE, (:>))
import Effectful qualified
import Effectful.Dispatch.Dynamic (EffectHandler, interpret)
import Effectful.Error.Static (Error, throwError)
import Effectful.Exception (ExitCase (..))
import Effectful.Exception qualified as Exception
import Kafka.Consumer (RdKafkaRespErrT (..))
import Kafka.Consumer qualified as K
import Kafka.Consumer.ConsumerProperties (ConsumerProperties)
import Kafka.Consumer.Subscription (Subscription)
import Kafka.Consumer.Types (ConsumerRecord (crTopic))
import Kafka.Effectful.Consumer.Classify (
    PollErrorDisposition (..),
    classifyPollError,
    isBenignCommitError,
 )
import Kafka.Effectful.Consumer.Effect (KafkaConsumer (..))
import Kafka.Effectful.OpenTelemetry.Propagation (
    extractTraceContextFromRecord,
 )
import Kafka.Effectful.OpenTelemetry.Semantic (
    consumerRecordAttributesWith,
    consumerSpanName,
 )
import Kafka.Types (KafkaError (..))
import OpenTelemetry.Context qualified as Context
import OpenTelemetry.Context.ThreadLocal (attachContext, detachContext)
import OpenTelemetry.SemanticsConfig (getSemanticsOptions, lookupStability)
import OpenTelemetry.Trace.Core (
    SpanArguments (kind),
    SpanKind (Consumer),
    Tracer,
    addAttributesToSpanArguments,
    defaultSpanArguments,
    inSpan'',
 )

{- | Run the 'KafkaConsumer' effect with OpenTelemetry tracing.

Identical in shape to 'Kafka.Effectful.Consumer.Interpreter.runKafkaConsumer',
plus an additional 'Tracer' argument. On every successful record return
from 'pollMessage' \/ 'pollMessageBatch' the interpreter:

* extracts a W3C trace context from the record\'s headers and
  attaches it as the current thread\'s 'OpenTelemetry.Context.Context';
* opens a Consumer-kind span named @\"process \<topic\>\"@,
  populated with the spec-aligned @messaging.*@ attribute set plus
  the @messaging.kafka.consumer.group@ attribute (read from the
  @group.id@ entry of the supplied 'ConsumerProperties').

Polls that time out (return @Nothing@) do not open a span. Per-record
errors in batch polls (@Left err@ entries) are kept in place in the
returned list and do not get spans either. The consumer handle is
acquired and released via 'Exception.generalBracket', exactly as
'runKafkaConsumer' does.

@since 0.2.0.0
-}
runKafkaConsumerTraced ::
    (IOE :> es, Error KafkaError :> es) =>
    Tracer ->
    ConsumerProperties ->
    Subscription ->
    Eff (KafkaConsumer : es) a ->
    Eff es a
runKafkaConsumerTraced tracer props sub action =
    fst
        <$> Exception.generalBracket
            acquire
            release
            ( \consumer ->
                interpret (handleTracedConsumer tracer props consumer) action
            )
  where
    acquire = do
        result <- Effectful.liftIO $ K.newConsumer props sub
        case result of
            Left err -> throwError err
            Right consumer -> pure consumer

    release consumer = \case
        ExitCaseSuccess _ -> do
            mbErr <- Effectful.liftIO $ K.closeConsumer consumer
            for_ mbErr throwError
        ExitCaseException _ ->
            Effectful.liftIO . void $ K.closeConsumer consumer
        ExitCaseAbort ->
            Effectful.liftIO . void $ K.closeConsumer consumer

handleTracedConsumer ::
    (IOE :> es, Error KafkaError :> es) =>
    Tracer ->
    ConsumerProperties ->
    K.KafkaConsumer ->
    EffectHandler KafkaConsumer es
handleTracedConsumer tracer props consumer _env = \case
    PollMessage timeout -> do
        result <- Effectful.liftIO $ K.pollMessage consumer timeout
        case result of
            Left err -> case classifyPollError err of
                PollTimeout -> pure Nothing
                PollBenign -> pure Nothing
                PollThrow -> throwError err
            Right cr -> Just <$> withConsumerSpan tracer props cr (pure cr)
    PollMessageEither timeout -> do
        result <- Effectful.liftIO $ K.pollMessage consumer timeout
        case result of
            Left err -> pure (Left err)
            Right cr -> Right <$> withConsumerSpan tracer props cr (pure cr)
    PollMessageBatch timeout batchSize -> do
        results <-
            Effectful.liftIO $
                K.pollMessageBatch consumer timeout batchSize
        traverse openSpanForResult results
      where
        openSpanForResult (Left err) = pure (Left err)
        openSpanForResult (Right cr) =
            Right <$> withConsumerSpan tracer props cr (pure cr)
    CommitOffsetMessage oc cr -> throwOnJustCommit $ K.commitOffsetMessage oc consumer cr
    CommitAllOffsets oc -> throwOnJustCommit $ K.commitAllOffsets oc consumer
    CommitPartitionsOffsets oc tps -> throwOnJustCommit $ K.commitPartitionsOffsets oc consumer tps
    StoreOffsets tps -> throwOnJust $ K.storeOffsets consumer tps
    StoreOffsetMessage cr -> throwOnJust $ K.storeOffsetMessage consumer cr
    Assign tps -> throwOnJust $ K.assign consumer tps
    PausePartitions parts ->
        throwOnKafkaErr (K.pausePartitions consumer parts)
    ResumePartitions parts ->
        throwOnKafkaErr (K.resumePartitions consumer parts)
    SeekPartitions tps timeout -> throwOnJust $ K.seekPartitions consumer tps timeout
    Committed timeout parts -> throwOnLeft $ K.committed consumer timeout parts
    Position parts -> throwOnLeft $ K.position consumer parts
    Assignment -> throwOnLeft $ K.assignment consumer
    Subscription -> throwOnLeft $ K.subscription consumer
    AskConsumerHandle -> pure consumer
  where
    throwOnJust action' = do
        mbErr <- Effectful.liftIO action'
        for_ mbErr throwError

    -- Commits get their own thrower: "nothing to commit" is a success.
    throwOnJustCommit action' = do
        mbErr <- Effectful.liftIO action'
        for_ mbErr $ \err ->
            if isBenignCommitError err then pure () else throwError err

    throwOnLeft action' = do
        result <- Effectful.liftIO action'
        case result of
            Left err -> throwError err
            Right a -> pure a

    throwOnKafkaErr action' = do
        err <- Effectful.liftIO action'
        case err of
            KafkaResponseError RdKafkaRespErrNoError -> pure ()
            _ -> throwError err

{- | Open a Consumer-kind span around an action that processes a single
record.

Extracts the W3C trace context from the record\'s headers, installs it as
the current thread context for the duration, then opens a span named
@\"process \<topic\>\"@ populated with the @messaging.*@ attribute
set (including @messaging.kafka.consumer.group@ when known).

Two details of the context handling are load-bearing.

The record\'s headers are extracted into 'Context.empty', /not/ into the
ambient thread-local context. That is what makes \"no headers → new root
span\" actually true. Extracting into the ambient context instead would
inherit whatever happens to be installed, which — immediately after another
traced record on the same thread — is that record\'s remote context, silently
chaining unrelated messages into one trace.

The attach is paired with its 'detachContext' token in a bracket, so the
caller\'s ambient context is restored however this returns. Without that, the
last record\'s context stays installed on the thread forever: it leaks into
subsequent records, into the rest of the batch walk, and into whatever the
application does after the poll.

Note that the span covers only the supplied action, which at both call sites
is @pure cr@ — so it is effectively a zero-duration marker at the point the
record was received, not a measurement of how long the record took to
process. Covering user processing would need a handler-wrapping API and is
deliberately out of scope here.
-}
withConsumerSpan ::
    (IOE :> es) =>
    Tracer ->
    ConsumerProperties ->
    ConsumerRecord (Maybe ByteString) (Maybe ByteString) ->
    Eff es a ->
    Eff es a
withConsumerSpan tracer props cr action = do
    semOpts <- Effectful.liftIO $ lookupStability "messaging" <$> getSemanticsOptions
    inboundCtx <-
        Effectful.liftIO $ extractTraceContextFromRecord cr Context.empty
    Exception.bracket
        (attachContext inboundCtx)
        detachContext
        ( \_token ->
            inSpan'' tracer (consumerSpanName (crTopic cr)) (spanArgs semOpts) $
                \_span -> action
        )
  where
    spanArgs semOpts =
        addAttributesToSpanArguments
            (consumerRecordAttributesWith semOpts props cr)
            defaultSpanArguments{kind = Consumer}
