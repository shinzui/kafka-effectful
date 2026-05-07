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

The design parallels the upstream
@hs-opentelemetry-instrumentation-hw-kafka-client@\'s
@OpenTelemetry.Instrumentation.Kafka.pollMessage@.

@since 0.2.0.0
-}
module Kafka.Effectful.OpenTelemetry.Consumer.Interpreter (
    -- * Interpreter
    runKafkaConsumerTraced,
)
where

import Control.Monad (void)
import Data.ByteString (ByteString)
import Data.Foldable (for_)
import Data.Map.Strict qualified as Map
import Effectful (Eff, IOE, (:>))
import Effectful qualified
import Effectful.Dispatch.Dynamic (EffectHandler, interpret)
import Effectful.Error.Static (Error, throwError)
import Effectful.Exception (ExitCase (..))
import Effectful.Exception qualified as Exception
import Kafka.Consumer (RdKafkaRespErrT (..))
import Kafka.Consumer qualified as K
import Kafka.Consumer.ConsumerProperties (ConsumerProperties (cpProps))
import Kafka.Consumer.Subscription (Subscription)
import Kafka.Consumer.Types (ConsumerRecord (crTopic))
import Kafka.Effectful.Consumer.Effect (KafkaConsumer (..))
import Kafka.Effectful.OpenTelemetry.Propagation (
    extractTraceContextFromRecord,
 )
import Kafka.Effectful.OpenTelemetry.Semantic (
    consumerRecordAttributes,
    consumerSpanName,
 )
import Kafka.Types (KafkaError (..))
import OpenTelemetry.Attributes.Attribute (toAttribute)
import OpenTelemetry.Attributes.Map (AttributeMap, insertAttributeByKey)
import OpenTelemetry.Context.ThreadLocal (attachContext, getContext)
import OpenTelemetry.SemanticConventions (messaging_kafka_consumer_group)
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
            Left (KafkaResponseError RdKafkaRespErrTimedOut) -> pure Nothing
            Left err -> throwError err
            Right cr -> Just <$> withConsumerSpan tracer props cr (pure cr)
    PollMessageBatch timeout batchSize -> do
        results <-
            Effectful.liftIO $
                K.pollMessageBatch consumer timeout batchSize
        traverse openSpanForResult results
      where
        openSpanForResult (Left err) = pure (Left err)
        openSpanForResult (Right cr) =
            Right <$> withConsumerSpan tracer props cr (pure cr)
    CommitOffsetMessage oc cr -> throwOnJust $ K.commitOffsetMessage oc consumer cr
    CommitAllOffsets oc -> throwOnJust $ K.commitAllOffsets oc consumer
    CommitPartitionsOffsets oc tps -> throwOnJust $ K.commitPartitionsOffsets oc consumer tps
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

Extracts the W3C trace context from the record\'s headers, attaches
it as the current thread context, then opens a span named
@\"process \<topic\>\"@ populated with the @messaging.*@ attribute
set (including @messaging.kafka.consumer.group@ when known).
-}
withConsumerSpan ::
    (IOE :> es) =>
    Tracer ->
    ConsumerProperties ->
    ConsumerRecord (Maybe ByteString) (Maybe ByteString) ->
    Eff es a ->
    Eff es a
withConsumerSpan tracer props cr action = do
    inboundCtx <- Effectful.liftIO $ do
        currentCtx <- getContext
        extractTraceContextFromRecord cr currentCtx
    void $ attachContext inboundCtx
    inSpan'' tracer (consumerSpanName (crTopic cr)) spanArgs $ \_span -> action
  where
    spanArgs =
        addAttributesToSpanArguments
            (withConsumerGroup (consumerRecordAttributes cr))
            defaultSpanArguments{kind = Consumer}

    withConsumerGroup :: AttributeMap -> AttributeMap
    withConsumerGroup attrs =
        case Map.lookup "group.id" (cpProps props) of
            Just groupId ->
                insertAttributeByKey
                    messaging_kafka_consumer_group
                    (toAttribute groupId)
                    attrs
            Nothing -> attrs
