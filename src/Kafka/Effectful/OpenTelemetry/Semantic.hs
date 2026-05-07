{- | Pure helpers that translate Kafka producer and consumer records
into the OpenTelemetry messaging-semantic-conventions attribute set.

The functions here do not perform any I\/O and do not depend on a
'Tracer'. They exist as building blocks for the traced interpreters
in "Kafka.Effectful.OpenTelemetry.Producer.Interpreter" and
"Kafka.Effectful.OpenTelemetry.Consumer.Interpreter", and are also
exposed so users who want to write a custom interpreter or a
framework wrapper can produce the same spec-aligned attribute set
without having to redefine the keys themselves.

The attribute keys and value types follow the OpenTelemetry messaging
spec v1.24 as exposed by @hs-opentelemetry-semantic-conventions@,
and they intentionally agree with what
@shibuya-kafka-adapter@\'s @Shibuya.Adapter.Kafka.Convert@ produces,
so the two libraries can be layered without attribute key drift.

@since 0.2.0.0
-}
module Kafka.Effectful.OpenTelemetry.Semantic () where
