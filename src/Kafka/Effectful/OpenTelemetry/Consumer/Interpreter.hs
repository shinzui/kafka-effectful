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

@since 0.2.0.0
-}
module Kafka.Effectful.OpenTelemetry.Consumer.Interpreter () where
