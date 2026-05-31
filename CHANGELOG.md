# Changelog

All notable changes to `kafka-effectful` are documented here.

This package follows the [Haskell Package Versioning Policy](https://pvp.haskell.org/).

## 0.3.0.0 — 2026-05-31

### Breaking Changes

- Upgrade OpenTelemetry support to the `hs-opentelemetry` 1.0 package
  family. The library now requires `hs-opentelemetry-api ^>=1.0`,
  `hs-opentelemetry-sdk ^>=1.0`, 1.0 exporters, and
  `hs-opentelemetry-semantic-conventions >=1.40 && <2`. Downstream
  build plans pinned to the 0.x package family must upgrade.

### New Features

- Add `producerRecordAttributesWith` and `consumerRecordAttributesWith`
  attribute builders that honor the semantic-convention stability mode.
- Add `kafkaHeadersToTextMap` and `textMapToKafkaHeaders` propagation
  helpers built on the OpenTelemetry 1.0 `TextMap` carrier.

### Other Changes

- Align Kafka messaging attributes with the v1.40 semantic-convention
  behavior used by `hs-opentelemetry-instrumentation-hw-kafka-client`
  1.0. Legacy messaging keys remain the default; set
  `OTEL_SEMCONV_STABILITY_OPT_IN=messaging` for stable names or
  `OTEL_SEMCONV_STABILITY_OPT_IN=messaging/dup` to emit both during
  migration.
- Propagation now uses the OpenTelemetry 1.0 `TextMap` carrier
  internally while preserving the existing request-header bridge
  helpers for callers that imported them directly.

## 0.2.0.0 — 2026-05-06

Additive release. No breaking changes to existing modules.

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
- Add `kafka-effectful-test` test suite covering the OpenTelemetry
  attribute helpers and trace-context propagation bridges.
- Add example projects: `example-sync-publish`,
  `example-transactional-etl`, and `example-otel-tracing`
  demonstrating an end-to-end traced producer/consumer pipeline.
- Expand the README with a "Producer scenarios" walkthrough covering
  all eight best-practice cases from upstream's
  `producer-best-practices.md`, and document the new tracing
  support.

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
