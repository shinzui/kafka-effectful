{- | Bridges between @hw-kafka-client@\'s 'Kafka.Types.Headers' and
OpenTelemetry propagation carriers, plus convenience helpers that
fetch the global propagator and inject\/extract trace context against
a record\'s headers in one call.

The traced interpreters use the @hs-opentelemetry-api@ 1.0
'TextMap' carrier. The older @http-types@ 'RequestHeaders' helpers
remain available for users who imported them directly.

@since 0.2.0.0
-}
module Kafka.Effectful.OpenTelemetry.Propagation (
    -- * Header bridges
    kafkaHeadersToTextMap,
    textMapToKafkaHeaders,
    kafkaHeadersToRequestHeaders,
    requestHeadersToKafkaHeaders,

    -- * W3C trace-context round-trip
    extractTraceContextFromRecord,
    injectTraceContextIntoRecord,
)
where

import Data.Bifunctor (first)
import Data.CaseInsensitive qualified as CI
import Data.Text.Encoding qualified as Text
import Kafka.Consumer.Types (ConsumerRecord (crHeaders))
import Kafka.Producer.Types (ProducerRecord (prHeaders))
import Kafka.Types (Headers, headersFromList, headersToList)
import Network.HTTP.Types (RequestHeaders)
import OpenTelemetry.Context (Context)
import OpenTelemetry.Propagator (
    TextMap,
    emptyTextMap,
    extract,
    getGlobalTextMapPropagator,
    inject,
    textMapFromList,
    textMapToList,
 )

{- | Convert @hw-kafka-client@ 'Headers' to the OpenTelemetry 1.0
'TextMap' propagation carrier.

Header names and values are decoded as UTF-8, matching upstream
@hs-opentelemetry-instrumentation-hw-kafka-client@ 1.0.
-}
kafkaHeadersToTextMap :: Headers -> TextMap
kafkaHeadersToTextMap =
    textMapFromList
        . map
            ( \(k, v) ->
                (Text.decodeUtf8 k, Text.decodeUtf8 v)
            )
        . headersToList

{- | Convert the OpenTelemetry 1.0 'TextMap' propagation carrier back
to @hw-kafka-client@ 'Headers'.
-}
textMapToKafkaHeaders :: TextMap -> Headers
textMapToKafkaHeaders =
    headersFromList
        . map
            ( \(k, v) ->
                (Text.encodeUtf8 k, Text.encodeUtf8 v)
            )
        . textMapToList

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

This fetches the global text-map propagator, hands it the record\'s
headers, and returns the resulting 'Context'. If the record carries
no @traceparent@ header the propagator returns the input 'Context'
unchanged.

This is the building block that the traced consumer interpreter uses
to root a per-message Consumer-kind span at the inbound trace
context.
-}
extractTraceContextFromRecord ::
    ConsumerRecord k v ->
    Context ->
    IO Context
extractTraceContextFromRecord record ctx = do
    propagator <- getGlobalTextMapPropagator
    extract propagator (kafkaHeadersToTextMap (crHeaders record)) ctx

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
    propagator <- getGlobalTextMapPropagator
    extraHeaders <- inject propagator ctx emptyTextMap
    let merged = prHeaders record <> textMapToKafkaHeaders extraHeaders
    pure record{prHeaders = merged}
