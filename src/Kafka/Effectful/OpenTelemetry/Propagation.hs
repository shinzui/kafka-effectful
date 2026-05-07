{- | Bridges between @hw-kafka-client@\'s 'Kafka.Types.Headers' (a list
of @(ByteString, ByteString)@) and @http-types@\'s 'RequestHeaders'
(a list of @(CI ByteString, ByteString)@), plus convenience helpers
that fetch the global propagator and inject\/extract a W3C trace
context against a record\'s headers in one call.

The header-bridge functions are pure and round-trip the underlying
bytes verbatim; only the case-sensitivity envelope changes (Kafka
headers are case-sensitive, HTTP headers are not).

@since 0.2.0.0
-}
module Kafka.Effectful.OpenTelemetry.Propagation (
    -- * Header bridges
    kafkaHeadersToRequestHeaders,
    requestHeadersToKafkaHeaders,

    -- * W3C trace-context round-trip
    extractTraceContextFromRecord,
    injectTraceContextIntoRecord,
)
where

import Data.Bifunctor (first)
import Data.CaseInsensitive qualified as CI
import Kafka.Consumer.Types (ConsumerRecord (crHeaders))
import Kafka.Producer.Types (ProducerRecord (prHeaders))
import Kafka.Types (Headers, headersFromList, headersToList)
import Network.HTTP.Types (RequestHeaders)
import OpenTelemetry.Context (Context)
import OpenTelemetry.Propagator (extract, inject)
import OpenTelemetry.Trace.Core (
    getGlobalTracerProvider,
    getTracerProviderPropagators,
 )

{- | Convert @hw-kafka-client@ 'Headers' (case-sensitive) to
@http-types@ 'RequestHeaders' (case-insensitive). Each
@(bsKey, bsVal)@ becomes @(CI.mk bsKey, bsVal)@.
-}
kafkaHeadersToRequestHeaders :: Headers -> RequestHeaders
kafkaHeadersToRequestHeaders = map (first CI.mk) . headersToList

{- | Convert @http-types@ 'RequestHeaders' (case-insensitive) back to
@hw-kafka-client@ 'Headers' (case-sensitive). The
'CI.foldedCase' lower-case form of each key is used as the
resulting Kafka header key.
-}
requestHeadersToKafkaHeaders :: RequestHeaders -> Headers
requestHeadersToKafkaHeaders = headersFromList . map (first CI.foldedCase)

{- | Extract a W3C trace context from a 'ConsumerRecord'\'s headers
and merge it into the supplied 'Context'.

This fetches the global tracer provider\'s propagator, hands it the
record\'s headers (after the case-insensitivity bridge), and returns
the resulting 'Context'. If the record carries no @traceparent@
header the propagator returns the input 'Context' unchanged.

This is the building block that the traced consumer interpreter uses
to root a per-message Consumer-kind span at the inbound trace
context.
-}
extractTraceContextFromRecord ::
    ConsumerRecord k v ->
    Context ->
    IO Context
extractTraceContextFromRecord record ctx = do
    propagator <- getTracerProviderPropagators <$> getGlobalTracerProvider
    extract propagator (kafkaHeadersToRequestHeaders (crHeaders record)) ctx

{- | Inject the supplied 'Context'\'s W3C trace context into a
'ProducerRecord'\'s headers, returning the augmented record.

Existing headers on the record are preserved; the propagator-emitted
headers (typically @traceparent@ and optionally @tracestate@) are
appended via 'Headers'\'s 'Semigroup' instance. The original record is
not mutated.

This is the building block that the traced producer interpreter uses
to publish the current span\'s context on the wire so that downstream
consumers can extract it and continue the trace.
-}
injectTraceContextIntoRecord ::
    Context ->
    ProducerRecord ->
    IO ProducerRecord
injectTraceContextIntoRecord ctx record = do
    propagator <- getTracerProviderPropagators <$> getGlobalTracerProvider
    extraHeaders <- inject propagator ctx []
    let merged = prHeaders record <> requestHeadersToKafkaHeaders extraHeaders
    pure record{prHeaders = merged}
