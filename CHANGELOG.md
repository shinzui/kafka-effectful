# Changelog

All notable changes to `kafka-effectful` are documented here.

This package follows the [Haskell Package Versioning Policy](https://pvp.haskell.org/).

## Unreleased

- Add `produceMessage'` and `produceMessageSync` to the
  `KafkaProducer` effect. `produceMessage'` mirrors
  `Kafka.Producer.produceMessage'`, taking a per-message
  `DeliveryReport -> IO ()` callback. `produceMessageSync` blocks
  until the broker acknowledges the record and returns the assigned
  `Offset`. Both throw `KafkaError` via the `Error` effect on
  failure.
- Add `produceMessageBatch` to the `KafkaProducer` effect. Returns
  only the records that failed to enqueue, paired with their
  `KafkaError`. The interpreter inlines the upstream definition
  (`mapM` over the list) because Hackage `hw-kafka-client-5.3.0`
  does not re-export `Kafka.Producer.produceMessageBatch`.
- Add the transaction API to the `KafkaProducer` effect —
  `initTransactions`, `beginTransaction`, `commitTransaction`,
  `abortTransaction` — plus the cross-effect helper
  `commitOffsetMessageTransaction` (in new module
  `Kafka.Effectful.Producer.Transaction`) that commits consumer
  offsets as part of the producer's open transaction. Re-exports
  `TxError` with its three accessors
  (`kafkaErrorTxnRequiresAbort`, `kafkaErrorIsRetriable`,
  `kafkaErrorIsFatal`).
- Add narrow handle-ask escape hatches `askProducerHandle` and
  `askConsumerHandle` on the scoped facades. Reachable only from
  `Kafka.Effectful.Producer` and `Kafka.Effectful.Consumer`; not
  re-exported from the combined `Kafka.Effectful` facade.
- Expand the README with a "Producer scenarios" walkthrough covering
  all eight best-practice cases from upstream's
  `producer-best-practices.md`.
- Add OpenTelemetry tracing support via opt-in interpreter variants
  `runKafkaProducerTraced` and `runKafkaConsumerTraced`. New modules
  under `Kafka.Effectful.OpenTelemetry.*` provide the
  attribute-builder helpers (`producerRecordAttributes`,
  `consumerRecordAttributes`) and the W3C trace-context header
  bridges (`extractTraceContextFromRecord`,
  `injectTraceContextIntoRecord`). The default interpreters
  `runKafkaProducer` and `runKafkaConsumer` are unchanged and remain
  zero-cost for users who do not want tracing. The attribute keys
  and value types match what `shibuya-kafka-adapter` already emits,
  so layering the two remains compatible.
- Bump version to 0.2.0.0 (additive minor change post-0.1: new
  modules and dependencies, no breaking changes to existing
  modules).

## 0.1.0.0 — 2026-04-16

Initial release.

This is an experimental release. Breaking changes are expected in
subsequent 0.x versions. Pin to an exact version in production until the
API stabilizes at 1.0.

- `KafkaProducer` effect with `produceMessage` and `flushProducer`
  operations.
- `KafkaConsumer` effect with polling, offset management, partition
  management, and querying operations.
- Resource-safe interpreters that acquire and release Kafka handles
  via `bracket`.
- Errors surfaced through `Effectful.Error.Static` as
  `Error KafkaError`.
