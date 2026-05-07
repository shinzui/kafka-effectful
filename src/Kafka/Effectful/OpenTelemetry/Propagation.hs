{- | Bridges between @hw-kafka-client@'s 'Kafka.Types.Headers' (a list
of @(ByteString, ByteString)@) and @http-types@'s 'RequestHeaders'
(a list of @(CI ByteString, ByteString)@), plus convenience helpers
that fetch the global propagator and inject\/extract a W3C trace
context against a record\'s headers in one call.

The header-bridge functions are pure and round-trip the underlying
bytes verbatim; only the case-sensitivity envelope changes (Kafka
headers are case-sensitive, HTTP headers are not).

@since 0.2.0.0
-}
module Kafka.Effectful.OpenTelemetry.Propagation () where
