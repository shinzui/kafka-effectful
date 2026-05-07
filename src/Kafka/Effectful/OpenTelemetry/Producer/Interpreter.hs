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

@since 0.2.0.0
-}
module Kafka.Effectful.OpenTelemetry.Producer.Interpreter () where
