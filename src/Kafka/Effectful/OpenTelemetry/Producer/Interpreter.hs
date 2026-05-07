-- The 'EffectHandler' type synonym in effectful-core expands to a
-- constraint that GHC's redundant-constraint check flags on the
-- handler's signature, even though the constraint is required for
-- 'interpret' to type-check. Suppress the warning at the file level.
{-# OPTIONS_GHC -Wno-redundant-constraints #-}

{- | OpenTelemetry-traced interpreter for the 'KafkaProducer' effect.

Drop-in alternative to 'Kafka.Effectful.Producer.Interpreter.runKafkaProducer'
that opens a Producer-kind span around every record-sending operation
('produceMessage', 'produceMessage'', 'produceMessageSync',
'produceMessageBatch'), populates the span with the spec-aligned
@messaging.*@ attribute set, and injects the current OTel context as
W3C @traceparent@\/@tracestate@ headers on the record before handing
it off to the underlying @hw-kafka-client@ produce call.

Non-sending operations (flush, transactional begin\/commit\/abort, etc.)
are passed through unchanged — they do not represent message sends and
therefore do not get a span.

The design parallels the upstream
@hs-opentelemetry-instrumentation-hw-kafka-client@\'s
@OpenTelemetry.Instrumentation.Kafka.produceMessage@.

@since 0.2.0.0
-}
module Kafka.Effectful.OpenTelemetry.Producer.Interpreter (
    -- * Interpreter
    runKafkaProducerTraced,
)
where

import Control.Concurrent.MVar qualified as Concurrent
import Data.Foldable (for_)
import Effectful (Eff, IOE, (:>))
import Effectful qualified
import Effectful.Dispatch.Dynamic (EffectHandler, interpret)
import Effectful.Error.Static (Error, throwError)
import Effectful.Exception qualified as Exception
import Kafka.Effectful.OpenTelemetry.Propagation (
    injectTraceContextIntoRecord,
 )
import Kafka.Effectful.OpenTelemetry.Semantic (
    producerRecordAttributes,
    producerSpanName,
 )
import Kafka.Effectful.Producer.Effect (KafkaProducer (..))
import Kafka.Producer (ProducerRecord (prTopic))
import Kafka.Producer qualified as K
import Kafka.Producer.ProducerProperties (ProducerProperties)
import Kafka.Transaction qualified as K
import Kafka.Types (KafkaError)
import OpenTelemetry.Context qualified as Context
import OpenTelemetry.Context.ThreadLocal (getContext)
import OpenTelemetry.Trace.Core (
    SpanArguments (kind),
    SpanKind (Producer),
    Tracer,
    addAttributesToSpanArguments,
    defaultSpanArguments,
    inSpan'',
 )

{- | Run the 'KafkaProducer' effect with OpenTelemetry tracing.

Identical in shape to 'Kafka.Effectful.Producer.Interpreter.runKafkaProducer',
plus an additional 'Tracer' argument used to open a Producer-kind
span around every record-sending operation. The current
'OpenTelemetry.Context.Context' is injected as W3C trace-context
headers on the outgoing record before the underlying
@hw-kafka-client@ produce call runs, so that downstream consumers
can extract the context and continue the trace.

The producer handle is acquired and released via 'Exception.bracket',
exactly as 'runKafkaProducer' does. Errors are thrown via the
'Error' effect.

@since 0.2.0.0
-}
runKafkaProducerTraced ::
    (IOE :> es, Error KafkaError :> es) =>
    Tracer ->
    ProducerProperties ->
    Eff (KafkaProducer : es) a ->
    Eff es a
runKafkaProducerTraced tracer props action =
    Exception.bracket
        acquire
        (Effectful.liftIO . K.closeProducer)
        (\producer -> interpret (handleTracedProducer tracer producer) action)
  where
    acquire = do
        result <- Effectful.liftIO $ K.newProducer props
        case result of
            Left err -> throwError err
            Right producer -> pure producer

handleTracedProducer ::
    (IOE :> es, Error KafkaError :> es) =>
    Tracer ->
    K.KafkaProducer ->
    EffectHandler KafkaProducer es
handleTracedProducer tracer producer _env = \case
    ProduceMessage record ->
        withProducerSpan tracer record $ \instrumentedRecord -> do
            mbErr <- Effectful.liftIO $ K.produceMessage producer instrumentedRecord
            for_ mbErr throwError
    ProduceMessage' record cb ->
        withProducerSpan tracer record $ \instrumentedRecord -> do
            res <-
                Effectful.liftIO $
                    K.produceMessage' producer instrumentedRecord cb
            case res of
                Left (K.ImmediateError err) -> throwError err
                Right () -> pure ()
    ProduceMessageSync record ->
        withProducerSpan tracer record $ \instrumentedRecord -> do
            var <- Effectful.liftIO Concurrent.newEmptyMVar
            res <-
                Effectful.liftIO $
                    K.produceMessage' producer instrumentedRecord (Concurrent.putMVar var)
            case res of
                Left (K.ImmediateError err) -> throwError err
                Right () -> do
                    Effectful.liftIO $ K.flushProducer producer
                    report <- Effectful.liftIO $ Concurrent.takeMVar var
                    case report of
                        K.DeliverySuccess _ offset -> pure offset
                        K.DeliveryFailure _ err -> throwError err
                        K.NoMessageError err -> throwError err
    ProduceMessageBatch records -> do
        -- One span per record so the Producer-kind attributes
        -- (partition, key) are per-record, matching the upstream
        -- reference. Spans are opened sequentially as the list is
        -- traversed.
        results <-
            traverse
                ( \r ->
                    withProducerSpan tracer r $ \instrumentedRecord -> do
                        mbErr <-
                            Effectful.liftIO $
                                K.produceMessage producer instrumentedRecord
                        pure (r, mbErr)
                )
                records
        pure [(r, err) | (r, Just err) <- results]
    FlushProducer ->
        Effectful.liftIO $ K.flushProducer producer
    InitTransactions timeout ->
        throwOnJust $ K.initTransactions producer timeout
    BeginTransaction ->
        throwOnJust $ K.beginTransaction producer
    CommitTransaction timeout ->
        Effectful.liftIO $ K.commitTransaction producer timeout
    AbortTransaction timeout ->
        throwOnJust $ K.abortTransaction producer timeout
    SendOffsetsToTransaction consumer record timeout ->
        Effectful.liftIO $
            K.commitOffsetMessageTransaction producer consumer record timeout
    AskProducerHandle ->
        pure producer
  where
    throwOnJust action' = do
        mbErr <- Effectful.liftIO action'
        for_ mbErr throwError

{- | Open a Producer-kind span around a record-sending action.

Builds the @messaging.*@ attribute set from the record, opens a
span named @\"send \<topic\>\"@, injects the current OTel context
as W3C trace-context headers onto a clone of the record, and runs
the supplied action with that instrumented record.
-}
withProducerSpan ::
    (IOE :> es) =>
    Tracer ->
    ProducerRecord ->
    (ProducerRecord -> Eff es a) ->
    Eff es a
withProducerSpan tracer record action =
    inSpan'' tracer (producerSpanName (prTopic record)) spanArgs $ \newSpan -> do
        ctx <- getContext
        instrumentedRecord <-
            Effectful.liftIO $
                injectTraceContextIntoRecord (Context.insertSpan newSpan ctx) record
        action instrumentedRecord
  where
    spanArgs =
        addAttributesToSpanArguments
            (producerRecordAttributes record)
            defaultSpanArguments{kind = Producer}
