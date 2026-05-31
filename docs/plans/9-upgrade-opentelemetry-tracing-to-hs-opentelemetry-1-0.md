---
id: 9
slug: upgrade-opentelemetry-tracing-to-hs-opentelemetry-1-0
title: "Upgrade OpenTelemetry tracing to hs-opentelemetry 1.0"
kind: exec-plan
created_at: 2026-05-31T20:08:25Z
intention: "intention_01ksztaa16ex2r3n1aapp19mxn"
---

# Upgrade OpenTelemetry tracing to hs-opentelemetry 1.0

This ExecPlan is a living document. The sections Progress, Surprises & Discoveries,
Decision Log, and Outcomes & Retrospective must be kept up to date as work proceeds.


## Purpose / Big Picture

`kafka-effectful` already has opt-in OpenTelemetry tracing modules under `src/Kafka/Effectful/OpenTelemetry/*`, but those modules and their tests are pinned to the older `hs-opentelemetry-api ^>=0.3`, `hs-opentelemetry-sdk ^>=0.1`, and `hs-opentelemetry-semantic-conventions ^>=0.1` package family. The local `mori` registry shows that `iand675/hs-opentelemetry` now provides `hs-opentelemetry-api 1.0.0.0`, `hs-opentelemetry-sdk 1.0.0.0`, exporters and propagators at `1.0.0.0`, and `hs-opentelemetry-semantic-conventions 1.40.0.0`, generated from OpenTelemetry semantic conventions v1.40.

After this plan is implemented, users can build `kafka-effectful` against the `hs-opentelemetry` 1.0 package family and keep using the same traced runner shape: `runKafkaProducerTraced tracer producerProps` for producer effects and `runKafkaConsumerTraced tracer consumerProps subscription` for consumer effects. Producer spans still describe Kafka sends, consumer spans still describe received records, and trace context still travels through Kafka headers. The observable upgrade is that the emitted messaging attributes follow the same v1.40 compatibility behavior used by `hs-opentelemetry-instrumentation-hw-kafka-client` 1.0: by default the library emits legacy messaging keys for compatibility, while `OTEL_SEMCONV_STABILITY_OPT_IN=messaging` emits stable keys and `OTEL_SEMCONV_STABILITY_OPT_IN=messaging/dup` emits both during migration.

The result is visible without a live Kafka broker by running the test suite. Tests should prove that the old default still emits keys such as `messaging.operation`, that stable mode emits `messaging.operation.name`, `messaging.operation.type`, and `messaging.consumer.group.name`, and that duplicate mode emits both legacy and stable names. With a local broker, `cabal run example-otel-tracing -f examples -- --bootstrap-servers localhost:9092 --topic otel-demo` should still print matching producer and consumer trace IDs.


## Progress

- [x] Milestone 1: Update dependency bounds and verify the 1.0 OpenTelemetry modules and types to use. Done 2026-05-31; `cabal build lib:kafka-effectful` resolved `hs-opentelemetry-api 1.0.0.0` and `hs-opentelemetry-semantic-conventions 1.40.0.0`.
- [x] Milestone 2: Migrate propagation helpers from the old request-header carrier to the `hs-opentelemetry-api` 1.0 text-map carrier. Done 2026-05-31; tracing now uses `TextMap` while request-header helpers remain exported.
- [x] Milestone 3: Migrate semantic attribute builders to the v1.40 messaging conventions and stability opt-in behavior. Done 2026-05-31; new `producerRecordAttributesWith` and `consumerRecordAttributesWith` helpers accept `StabilityOpt`.
- [x] Milestone 4: Update traced producer and consumer interpreters to use the migrated helpers and record errors consistently with upstream Kafka instrumentation. Done 2026-05-31; producer errors add `error.type` and set span status to `Error`.
- [x] Milestone 5: Update tests, README, CHANGELOG, and the example so the new behavior is documented and verifiable. Done 2026-05-31; tests now cover legacy, stable, and duplicate semantic modes, README and CHANGELOG describe the 1.0 migration, and the example builds against the 1.0 shutdown API.
- [x] Milestone 6: Run validation commands, record results, and fill in Outcomes & Retrospective. Done 2026-05-31; library build, test suite, and example build pass. Live broker run was skipped because `nc -z localhost 9092` reported no listener.


## Surprises & Discoveries

- 2026-05-31: `mori registry show iand675/hs-opentelemetry --full` reports `hs-opentelemetry-api 1.0.0.0` and `hs-opentelemetry-semantic-conventions 1.40.0.0`. The current `kafka-effectful.cabal` still depends on `hs-opentelemetry-api ^>=0.3`, `hs-opentelemetry-sdk ^>=0.1`, `hs-opentelemetry-exporter-* ^>=0.1` or `^>=0.0.1`, and semantic conventions `^>=0.1`, so a bounds-only bump is required before compilation can select the 1.0 package family.
- 2026-05-31: The local 1.0 `OpenTelemetry.Instrumentation.Kafka` source in `/Users/shinzui/Keikaku/hub/haskell/hs-opentelemetry-project/hs-opentelemetry/instrumentation/hw-kafka-client/src/OpenTelemetry/Instrumentation/Kafka.hs` gates messaging operation and consumer-group keys through `OpenTelemetry.SemanticsConfig.lookupStability "messaging"`. Default `Old` mode emits legacy keys, `Stable` mode emits stable keys, and `StableAndOld` mode emits both.
- 2026-05-31: The generated v1.40 semantic conventions module marks `messaging.operation`, `messaging.kafka.consumer.group`, `messaging.kafka.destination.partition`, and `messaging.kafka.message.offset` as deprecated, but the 1.0 upstream Kafka instrumentation still emits the partition and offset keys while adding stable operation and consumer group alternatives. This plan follows the package's own Kafka instrumentation behavior rather than inventing a different Kafka attribute policy.
- 2026-05-31: Cabal rejected the plan's original `cabal build kafka-effectful:lib` command with `Unknown target 'kafka-effectful:lib'`. The correct component target for this package is `cabal build lib:kafka-effectful`, which successfully built the library against OpenTelemetry 1.0.
- 2026-05-31: `examples/OtelTracing.hs` needed a 1.0 SDK API update beyond dependency bounds. `shutdownTracerProvider` now requires a timeout argument and returns a shutdown result, so the example now calls `shutdownTracerProvider tp Nothing` and ignores the result.


## Decision Log

- Decision: Keep `kafka-effectful`'s traced interpreter API stable and migrate internals, tests, and docs around it.
  Rationale: The existing public affordance is already opt-in and useful. Users should not have to rewrite effect-level producer or consumer code just because the OpenTelemetry package family moved to 1.0.
  Date: 2026-05-31

- Decision: Match `hs-opentelemetry-instrumentation-hw-kafka-client` 1.0's messaging stability behavior instead of hard-switching all users to stable-only attribute names.
  Rationale: The upstream package's own Kafka instrumentation is the closest source of truth for how `hs-opentelemetry` 1.0 expects Kafka spans to behave. Emitting legacy keys by default preserves dashboards during upgrade, while `OTEL_SEMCONV_STABILITY_OPT_IN=messaging` and `messaging/dup` give users the stable v1.40 migration path.
  Date: 2026-05-31

- Decision: Continue to keep OpenTelemetry support inside the main `kafka-effectful` library for this upgrade.
  Rationale: The modules are already exposed by the main package in `kafka-effectful.cabal`, and this plan is an upgrade of existing behavior rather than a packaging split. A separate package would add release coordination without reducing risk for this change.
  Date: 2026-05-31


## Outcomes & Retrospective

Implemented the OpenTelemetry 1.0 upgrade. `kafka-effectful.cabal` now accepts the `hs-opentelemetry` 1.0 API, SDK, exporters, and `hs-opentelemetry-semantic-conventions >=1.40 && <2`. Propagation uses the 1.0 `TextMap` carrier internally while preserving the older request-header helper functions. Semantic attribute helpers now support `Old`, `Stable`, and `StableAndOld` messaging modes through `producerRecordAttributesWith` and `consumerRecordAttributesWith`, and traced interpreters read `OTEL_SEMCONV_STABILITY_OPT_IN` through `getSemanticsOptions`.

Validation completed on 2026-05-31:

```text
cabal build lib:kafka-effectful
Result: passed

cabal test kafka-effectful-test
Result: passed, All 21 tests passed

cabal build example-otel-tracing -f examples
Result: passed
```

The live broker example was not run because `nc -z localhost 9092` returned exit code 1, indicating no Kafka broker was listening locally. The automated build and test coverage still prove the dependency upgrade, semantic-mode behavior, and propagation helpers.


## Context and Orientation

This repository is a Haskell library. Its package definition is `kafka-effectful.cabal`. The library wraps `hw-kafka-client` with `effectful` effects. A producer effect is an abstract operation set for sending messages; its effect definition lives in `src/Kafka/Effectful/Producer/Effect.hs`, and the normal non-tracing interpreter lives in `src/Kafka/Effectful/Producer/Interpreter.hs`. A consumer effect is an abstract operation set for polling records and managing offsets; its effect definition lives in `src/Kafka/Effectful/Consumer/Effect.hs`, and the normal non-tracing interpreter lives in `src/Kafka/Effectful/Consumer/Interpreter.hs`.

OpenTelemetry is an observability API for traces, metrics, and logs. A trace is a tree of spans. A span is a timed operation with attributes. Context propagation is how a trace parent is passed across process boundaries; for Kafka this repository stores propagation headers on `hw-kafka-client` records. The existing OpenTelemetry facade is `src/Kafka/Effectful/OpenTelemetry.hs`. Producer tracing is implemented in `src/Kafka/Effectful/OpenTelemetry/Producer/Interpreter.hs`, consumer tracing is implemented in `src/Kafka/Effectful/OpenTelemetry/Consumer/Interpreter.hs`, propagation helpers are in `src/Kafka/Effectful/OpenTelemetry/Propagation.hs`, and pure attribute builders are in `src/Kafka/Effectful/OpenTelemetry/Semantic.hs`.

The current `Semantic.hs` imports old semantic convention keys: `messaging_system`, `messaging_destination_name`, `messaging_operation`, `messaging_kafka_destination_partition`, `messaging_kafka_message_offset`, and `messaging_kafka_message_key`. Those keys are still present in semantic conventions 1.40, but some are deprecated. The 1.0 Kafka instrumentation in `hs-opentelemetry` imports additional keys: `error_type`, `messaging_client_id`, `messaging_consumer_group_name`, `messaging_message_body_size`, `messaging_operation_name`, and `messaging_operation_type`. It also imports `OpenTelemetry.SemanticsConfig (StabilityOpt (..), getSemanticsOptions, lookupStability)` and chooses operation and consumer-group keys based on `lookupStability "messaging"`.

The current `Propagation.hs` bridges Kafka headers to `Network.HTTP.Types.RequestHeaders` and calls `OpenTelemetry.Propagator.extract` and `OpenTelemetry.Propagator.inject` through the global tracer provider's propagators. In the local 1.0 source, Kafka instrumentation instead uses `OpenTelemetry.Propagator.TextMap`, `emptyTextMap`, `textMapFromList`, `textMapToList`, `getGlobalTextMapPropagator`, `extract`, and `inject`. That is the API this plan should migrate to. A text map is the carrier format the 1.0 propagator API reads and writes; for Kafka headers it is built by UTF-8 decoding `(ByteString, ByteString)` header pairs into `(Text, Text)` pairs and encoding them back after injection.

Tests currently live under `test/Kafka/Effectful/OpenTelemetry/*`. `SemanticTest.hs` asserts legacy keys only. `PropagationTest.hs` initializes a global tracer provider and verifies W3C `traceparent` round trips. `ShibuyaCompatibilityTest.hs` pins a small set of legacy keys for compatibility with `shibuya-kafka-adapter`. These tests must be updated to verify the 1.0 compatibility modes rather than assuming a single hard-coded attribute set.

The previous plan `docs/plans/8-add-opentelemetry-tracing-support.md` is checked in and records why the traced interpreters exist. It is useful context, but this plan is self-contained and should be enough to execute the upgrade without reading that older plan.


## Plan of Work

Milestone 1 updates dependency edges and confirms the target API. Edit `kafka-effectful.cabal` so every `hs-opentelemetry-*` dependency in the library, test suite, and `example-otel-tracing` executable accepts 1.0. Use `hs-opentelemetry-api ^>=1.0`, `hs-opentelemetry-sdk ^>=1.0`, `hs-opentelemetry-exporter-otlp ^>=1.0`, `hs-opentelemetry-exporter-in-memory ^>=1.0`, and `hs-opentelemetry-semantic-conventions >=1.40 && <2`. Keep `http-types` because the existing public `kafkaHeadersToRequestHeaders` and `requestHeadersToKafkaHeaders` helpers should remain available as source-compatible bridge helpers, even though the traced interpreters will use the 1.0 `TextMap` carrier internally. The milestone is complete when `cabal build kafka-effectful:lib` gets past dependency solving or fails only on source API errors expected from the migration.

Milestone 2 migrates `src/Kafka/Effectful/OpenTelemetry/Propagation.hs`. Add a `TextMap` bridge matching the 1.0 upstream Kafka instrumentation and make tracing use it. Export `kafkaHeadersToTextMap :: Headers -> TextMap` and `textMapToKafkaHeaders :: TextMap -> Headers`. Keep `kafkaHeadersToRequestHeaders :: Headers -> RequestHeaders` and `requestHeadersToKafkaHeaders :: RequestHeaders -> Headers` exactly as public compatibility helpers for users that already imported them, but do not use those helpers in `extractTraceContextFromRecord` or `injectTraceContextIntoRecord`. Update `extractTraceContextFromRecord` to call `getGlobalTextMapPropagator` and `extract propagator (kafkaHeadersToTextMap (crHeaders record)) ctx`. Update `injectTraceContextIntoRecord` to call `inject propagator ctx emptyTextMap` and append `textMapToKafkaHeaders` to the existing `prHeaders`.

Milestone 3 migrates `src/Kafka/Effectful/OpenTelemetry/Semantic.hs`. Introduce `producerRecordAttributesWith :: StabilityOpt -> ProducerRecord -> AttributeMap` and `consumerRecordAttributesWith :: StabilityOpt -> ConsumerProperties -> ConsumerRecord (Maybe ByteString) (Maybe ByteString) -> AttributeMap`. Keep `producerRecordAttributes :: ProducerRecord -> AttributeMap` and `consumerRecordAttributes :: ConsumerRecord (Maybe ByteString) (Maybe ByteString) -> AttributeMap` as legacy-default pure helpers for existing users, but have traced interpreters call the `With` variants after reading `lookupStability "messaging" <$> getSemanticsOptions`. Producer attributes should always include `messaging.system="kafka"`, `messaging.destination.name=<topic>`, `messaging.kafka.message.key` when the key decodes as UTF-8, `messaging.kafka.destination.partition` for `SpecifiedPartition`, and `messaging.message.body.size` when `prValue` is present. Operation attributes should be `messaging.operation="send"` in `Old`, `messaging.operation.name="send"` plus `messaging.operation.type="send"` in `Stable`, and all three in `StableAndOld`. Consumer attributes should always include the same common fields for topic and key, `messaging.kafka.destination.partition`, `messaging.kafka.message.offset`, and `messaging.message.body.size` when `crValue` is present. Consumer properties should add `messaging.client.id` when `client.id` exists in `cpProps`. Consumer group should use `messaging.kafka.consumer.group` in `Old`, `messaging.consumer.group.name` in `Stable`, and both in `StableAndOld`.

Milestone 4 updates the traced interpreters. In `src/Kafka/Effectful/OpenTelemetry/Producer/Interpreter.hs`, read the messaging stability option once per span and build span arguments from `producerRecordAttributesWith`. When an underlying produce operation returns or reports a Kafka error inside the producer span, add `error.type` and set span status to `Error`, mirroring the local 1.0 `OpenTelemetry.Instrumentation.Kafka.produceMessage` source. In `src/Kafka/Effectful/OpenTelemetry/Consumer/Interpreter.hs`, stop adding the consumer-group attribute locally; pass `ConsumerProperties` into `consumerRecordAttributesWith` so all consumer attributes are decided in one place. Keep timeout behavior unchanged: `pollMessage` returning `Left (KafkaResponseError RdKafkaRespErrTimedOut)` still returns `Nothing` and emits no span.

Milestone 5 updates tests and docs. Revise `test/Kafka/Effectful/OpenTelemetry/SemanticTest.hs` so it tests legacy, stable, and duplicate modes by calling the new `With` helpers directly with `Old`, `Stable`, and `StableAndOld`. Add assertions for `messaging.message.body.size`, `messaging.client.id`, `messaging.consumer.group.name`, and absence or presence of legacy keys per mode. Revise `test/Kafka/Effectful/OpenTelemetry/PropagationTest.hs` to use `TextMap` helper names and the 1.0 propagator API. Revise `test/Kafka/Effectful/OpenTelemetry/ShibuyaCompatibilityTest.hs` to state that Shibuya compatibility is a legacy-mode compatibility pin if Shibuya still emits legacy keys. Update `README.md` to describe the new `OTEL_SEMCONV_STABILITY_OPT_IN` behavior and stable keys. Update `CHANGELOG.md` with the 1.0 dependency upgrade and semantic-convention migration notes. Update `examples/OtelTracing.hs` only if imports or initialization APIs changed.

Milestone 6 validates and records evidence. Run the library build, the test suite, and the example build. If a local Kafka broker is available, run the end-to-end example and record the matching trace IDs in this plan's Outcomes & Retrospective. If no broker is available, record that the live example was not run and that validation stopped at build/test.


## Concrete Steps

Start from the repository root:

```bash
cd /Users/shinzui/Keikaku/bokuno/kafka-effectful
```

Confirm the current project identity and dependency source locations. These commands are safe to repeat and should not search `/` or `/nix/store`:

```bash
mori show --full
mori registry search hs-opentelemetry
mori registry show iand675/hs-opentelemetry --full
mori registry docs iand675/hs-opentelemetry
```

The relevant expected facts are:

```text
hs-opentelemetry-api version: 1.0.0.0
hs-opentelemetry-sdk version: 1.0.0.0
hs-opentelemetry-exporter-in-memory version: 1.0.0.0
hs-opentelemetry-exporter-otlp version: 1.0.0.0
hs-opentelemetry-semantic-conventions version: 1.40.0.0
```

Read the upstream 1.0 Kafka instrumentation and semantic configuration source before editing:

```bash
sed -n '1,560p' /Users/shinzui/Keikaku/hub/haskell/hs-opentelemetry-project/hs-opentelemetry/instrumentation/hw-kafka-client/src/OpenTelemetry/Instrumentation/Kafka.hs
sed -n '1,220p' /Users/shinzui/Keikaku/hub/haskell/hs-opentelemetry-project/hs-opentelemetry/api/src/OpenTelemetry/SemanticsConfig.hs
sed -n '27600,28140p' /Users/shinzui/Keikaku/hub/haskell/hs-opentelemetry-project/hs-opentelemetry/semantic-conventions/src/OpenTelemetry/SemanticConventions.hs
```

Edit the Cabal file and OpenTelemetry modules described in Plan of Work. After the first pass, run:

```bash
cabal build lib:kafka-effectful
```

Expected success shape:

```text
Build profile: -w ghc-...
In order, the following will be built ...
...
Build completed successfully
```

If the build fails with missing imports, use `rg` scoped to the local `hs-opentelemetry` source path to confirm the correct module name or exported function. Do not search `/` or `/nix/store`.

Run the test suite:

```bash
cabal test kafka-effectful-test
```

Expected success shape:

```text
Kafka.Effectful.OpenTelemetry
  Semantic
    ...
  Propagation
    ...
  Shibuya compatibility
    ...
All ... tests passed
```

Build the example:

```bash
cabal build example-otel-tracing -f examples
```

If a local Kafka broker is available at `localhost:9092`, run:

```bash
cabal run example-otel-tracing -f examples -- --bootstrap-servers localhost:9092 --topic otel-demo
```

Expected successful output includes two identical trace IDs:

```text
[otel-tracing] producer trace id: <32 hex chars>
[otel-tracing] consumer trace id: <same 32 hex chars>
[otel-tracing] trace IDs match - context propagated through Kafka headers
```


## Validation and Acceptance

The dependency upgrade is accepted when `kafka-effectful.cabal` no longer restricts any `hs-opentelemetry-*` package to the pre-1.0 package family and `cabal build lib:kafka-effectful` succeeds with `hs-opentelemetry-api 1.0.0.0` and `hs-opentelemetry-semantic-conventions 1.40.0.0`.

The semantic-convention migration is accepted when tests prove three modes. In default legacy mode, producer attributes include `messaging.operation="send"` and do not include `messaging.operation.name` or `messaging.operation.type`; consumer attributes include `messaging.kafka.consumer.group` when `group.id` is set and do not include `messaging.consumer.group.name`. In stable mode, producer and consumer attributes include `messaging.operation.name` and `messaging.operation.type`, omit `messaging.operation`, include `messaging.consumer.group.name` for consumers, and omit `messaging.kafka.consumer.group`. In duplicate mode, both old and stable operation and consumer-group keys appear.

The propagation migration is accepted when `PropagationTest` proves that a known W3C `traceparent` can be injected into `ProducerRecord.prHeaders` and extracted from `ConsumerRecord.crHeaders` using the 1.0 global text-map propagator. The test should continue to verify the exact trace ID from the W3C example value `00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01`.

The user-facing tracing behavior is accepted when the example still produces matching producer and consumer trace IDs against a local broker. If no broker is available during implementation, compilation and tests are sufficient for this plan's automated validation, but the missing live broker check must be recorded in Outcomes & Retrospective.


## Idempotence and Recovery

All `mori`, `sed`, `rg`, `cabal build`, and `cabal test` commands in this plan are safe to repeat. They do not mutate project source files. The code edits are ordinary source changes; if a step fails, inspect the compiler error, compare the referenced API against the local `hs-opentelemetry` source found through `mori`, and retry the same build command.

Do not run destructive cleanup commands to recover from build failures. If Cabal's build cache appears stale, prefer `cabal clean` only after checking that no useful build output is needed for debugging. Do not modify `/nix/store`, and do not search or traverse `/` or `/nix/store`.

The working tree currently has an untracked `mina.kdl` file unrelated to this plan. Leave it alone unless the user explicitly asks to include or remove it.


## Interfaces and Dependencies

At the end of the work, `kafka-effectful.cabal` should allow the 1.0 OpenTelemetry family:

```cabal
hs-opentelemetry-api                   ^>=1.0
hs-opentelemetry-sdk                   ^>=1.0
hs-opentelemetry-exporter-otlp         ^>=1.0
hs-opentelemetry-exporter-in-memory    ^>=1.0
hs-opentelemetry-semantic-conventions  >=1.40 && <2
```

The public facade `Kafka.Effectful.OpenTelemetry` should still export `runKafkaProducerTraced`, `runKafkaConsumerTraced`, `producerRecordAttributes`, `consumerRecordAttributes`, `producerSpanName`, `consumerSpanName`, `extractTraceContextFromRecord`, and `injectTraceContextIntoRecord`. It should additionally export the stability-aware helpers if those are useful for tests and custom instrumentation:

```haskell
producerRecordAttributesWith ::
    StabilityOpt ->
    ProducerRecord ->
    AttributeMap

consumerRecordAttributesWith ::
    StabilityOpt ->
    ConsumerProperties ->
    ConsumerRecord (Maybe ByteString) (Maybe ByteString) ->
    AttributeMap
```

`Kafka.Effectful.OpenTelemetry.Propagation` should expose a 1.0-native carrier bridge while preserving the existing request-header helpers:

```haskell
kafkaHeadersToTextMap :: Headers -> TextMap
textMapToKafkaHeaders :: TextMap -> Headers
kafkaHeadersToRequestHeaders :: Headers -> RequestHeaders
requestHeadersToKafkaHeaders :: RequestHeaders -> Headers
extractTraceContextFromRecord :: ConsumerRecord k v -> Context -> IO Context
injectTraceContextIntoRecord :: Context -> ProducerRecord -> IO ProducerRecord
```

`Kafka.Effectful.OpenTelemetry.Producer.Interpreter.runKafkaProducerTraced` should keep this signature:

```haskell
runKafkaProducerTraced ::
    (IOE :> es, Error KafkaError :> es) =>
    Tracer ->
    ProducerProperties ->
    Eff (KafkaProducer : es) a ->
    Eff es a
```

`Kafka.Effectful.OpenTelemetry.Consumer.Interpreter.runKafkaConsumerTraced` should keep this signature:

```haskell
runKafkaConsumerTraced ::
    (IOE :> es, Error KafkaError :> es) =>
    Tracer ->
    ConsumerProperties ->
    Subscription ->
    Eff (KafkaConsumer : es) a ->
    Eff es a
```

The new code should import `OpenTelemetry.SemanticsConfig` from `hs-opentelemetry-api 1.0` and semantic keys from `OpenTelemetry.SemanticConventions` from `hs-opentelemetry-semantic-conventions 1.40`. It should use `OpenTelemetry.Propagator.TextMap` and `getGlobalTextMapPropagator` for propagation. It should not depend on `hs-opentelemetry-instrumentation-hw-kafka-client`; that package remains a reference implementation, not a runtime dependency.
