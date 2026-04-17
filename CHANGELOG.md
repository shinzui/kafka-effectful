# Changelog

All notable changes to `kafka-effectful` are documented here.

This package follows the [Haskell Package Versioning Policy](https://pvp.haskell.org/).

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
