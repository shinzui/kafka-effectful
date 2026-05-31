{- | Single-import facade for the OpenTelemetry-aware variants of
@kafka-effectful@\'s producer and consumer interpreters, plus the
pure attribute-builder helpers and W3C trace-context propagation
helpers that the traced interpreters are built from.

Typical wiring:

> import Kafka.Effectful.OpenTelemetry
> import OpenTelemetry.Trace (initializeGlobalTracerProvider, makeTracer, tracerOptions)
>
> tracer <- do
>   tp <- initializeGlobalTracerProvider
>   pure (makeTracer tp \"my-app\" tracerOptions)
>
> runEff . runError . runKafkaProducerTraced tracer producerProps $ do
>   produceMessage record

The facade does not re-export the upstream @Tracer@, @Span@, etc.
types; users who need those import them from @OpenTelemetry.Trace@
or @OpenTelemetry.Trace.Core@ directly.

@since 0.2.0.0
-}
module Kafka.Effectful.OpenTelemetry (
    -- * Traced interpreters
    runKafkaProducerTraced,
    runKafkaConsumerTraced,

    -- * Attribute helpers
    producerRecordAttributes,
    producerRecordAttributesWith,
    consumerRecordAttributes,
    consumerRecordAttributesWith,
    producerSpanName,
    consumerSpanName,

    -- * Trace-context propagation
    extractTraceContextFromRecord,
    injectTraceContextIntoRecord,
    kafkaHeadersToTextMap,
    textMapToKafkaHeaders,
    kafkaHeadersToRequestHeaders,
    requestHeadersToKafkaHeaders,
)
where

import Kafka.Effectful.OpenTelemetry.Consumer.Interpreter (runKafkaConsumerTraced)
import Kafka.Effectful.OpenTelemetry.Producer.Interpreter (runKafkaProducerTraced)
import Kafka.Effectful.OpenTelemetry.Propagation (
    extractTraceContextFromRecord,
    injectTraceContextIntoRecord,
    kafkaHeadersToRequestHeaders,
    kafkaHeadersToTextMap,
    requestHeadersToKafkaHeaders,
    textMapToKafkaHeaders,
 )
import Kafka.Effectful.OpenTelemetry.Semantic (
    consumerRecordAttributes,
    consumerRecordAttributesWith,
    consumerSpanName,
    producerRecordAttributes,
    producerRecordAttributesWith,
    producerSpanName,
 )
