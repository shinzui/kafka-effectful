{- | Single-import facade for the OpenTelemetry-aware variants of
@kafka-effectful@\'s producer and consumer interpreters, plus the
pure attribute-builder helpers and W3C trace-context propagation
helpers that the traced interpreters are built from.

@since 0.2.0.0
-}
module Kafka.Effectful.OpenTelemetry () where
