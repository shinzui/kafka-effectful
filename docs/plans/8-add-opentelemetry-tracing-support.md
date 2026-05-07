# Add OpenTelemetry tracing support compatible with shibuya-kafka-adapter

Intention: intention_01kr03xss6ejgs4n4ssz3vr2qv

This ExecPlan is a living document. The sections Progress, Surprises & Discoveries,
Decision Log, and Outcomes & Retrospective must be kept up to date as work proceeds.

This document is maintained in accordance with `.claude/skills/exec-plan/PLANS.md`.


## Purpose / Big Picture

Today the `kafka-effectful` library at version `0.1.0.0` provides two `effectful`
effects — `KafkaProducer` and `KafkaConsumer` — and two interpreters,
`runKafkaProducer` and `runKafkaConsumer`, that wrap `hw-kafka-client`. Neither
the effects nor the interpreters know anything about OpenTelemetry. A user who
wants distributed tracing must currently write the tracing themselves — either
by importing the upstream package
`hs-opentelemetry-instrumentation-hw-kafka-client` and calling its
`OpenTelemetry.Instrumentation.Kafka.{produceMessage, pollMessage}` against the
raw `hw-kafka-client` API (bypassing this library), or by composing helpers
from a downstream framework (as `shibuya-kafka-adapter` does in
`shibuya-project/shibuya-kafka-adapter/shibuya-kafka-adapter/src/Shibuya/Adapter/Kafka/Convert.hs`).
The masterplan
`docs/masterplans/1-prepare-kafka-effectful-0-1-release.md` deferred
"OpenTelemetry-traced interpreter variants" to post-0.1, and that gap is the
subject of this plan.

After this plan completes, an application that uses `kafka-effectful` can:

- Swap `runKafkaProducer` for `runKafkaProducerTraced tracer` (a new
  interpreter variant that takes an OTel `Tracer`) and have every
  `produceMessage`, `produceMessage'`, `produceMessageSync`, and
  `produceMessageBatch` call automatically open a `Producer`-kind OTel span
  named `"send <topic>"`. The interpreter populates the span with the
  spec-mandated `messaging.*` attribute set and injects the current OTel
  context as W3C `traceparent`/`tracestate` headers on the `ProducerRecord`
  before handing the record off to the underlying `Kafka.Producer.produceMessage`.
  Because the interpreter wraps the existing operations from
  `Kafka.Effectful.Producer.Effect`, the user's effect-level code does not
  change — only the call to the runner does.

- Similarly swap `runKafkaConsumer` for `runKafkaConsumerTraced tracer` and
  have every successful `pollMessage` / `pollMessageBatch` open a
  `Consumer`-kind span named `"process <topic>"` rooted at the W3C trace
  context carried by the record's headers (or as a new root span when no
  inbound context is present). The span carries `messaging.system=kafka`,
  `messaging.destination.name=<topic>`, `messaging.operation=process`,
  `messaging.kafka.destination.partition` (Int64),
  `messaging.kafka.message.offset` (Int64), and — when present and
  UTF-8-decodable — `messaging.kafka.message.key` (Text). `pollMessage`'s
  timeout-returns-`Nothing` semantics are preserved (no span is opened on
  timeout).

- Reach the same building blocks the traced interpreters use, in case the
  user wants to lift them into a custom interpreter or a framework like
  Shibuya. A new module `Kafka.Effectful.OpenTelemetry.Semantic` exposes pure
  attribute-builder functions
  (`producerRecordAttributes :: ProducerRecord -> AttributeMap`,
  `consumerRecordAttributes :: ConsumerRecord (Maybe ByteString) (Maybe ByteString) -> AttributeMap`)
  that produce exactly the spec-aligned attribute set — the same keys and
  value types that `shibuya-kafka-adapter`'s `Convert.hs` is already using.
  A second module `Kafka.Effectful.OpenTelemetry.Propagation` exposes header
  bridges
  (`extractTraceContextHeaders :: Headers -> RequestHeaders` and
  `injectTraceContextHeaders :: RequestHeaders -> Headers`) so users can
  call the W3C propagator without having to reinvent the
  `case-insensitive` ↔ `hw-kafka-client.Headers` plumbing.

- Continue to use `runKafkaProducer` and `runKafkaConsumer` unchanged. The
  existing default interpreters stay zero-cost for users who do not want
  OTel, and the new modules are additive — code that does not import
  `Kafka.Effectful.OpenTelemetry.*` still works exactly as before.

To see this working when the plan is complete, a reader runs three checks:

1.  `cabal build all` is clean against the existing GHC 9.12 toolchain
    declared in `flake.nix`. The two new traced interpreters and three new
    helper modules type-check, and the existing `Kafka.Effectful.*` modules
    are unchanged at the source level (the cabal file gains
    `exposed-modules` entries, build-depends, and a new test-suite, but the
    existing modules' contents do not change).

2.  `cabal test` succeeds against a new test-suite `kafka-effectful-test`
    that exercises the helper modules with an in-memory OTel exporter from
    `hs-opentelemetry-exporter-in-memory`. The tests prove that
    `producerRecordAttributes` and `consumerRecordAttributes` emit the
    spec-aligned key set, that `extractTraceContextHeaders` /
    `injectTraceContextHeaders` round-trip a known `traceparent` correctly,
    and that the public-facing attribute keys and value types match what
    `shibuya-kafka-adapter`'s `Convert.hs` already produces (compatibility
    pin — if upstream changes a key, both projects break together, not
    silently).

3.  `cabal run example-otel-tracing -- --bootstrap-servers localhost:9092
    --topic otel-demo` against a local Kafka broker (the same `process-up`
    workflow that `shibuya-kafka-adapter`'s OTel demo uses) sends one record
    through the traced producer and reads it back through the traced
    consumer. The example prints the producer-side trace ID, then the
    consumer-side trace ID; the two **must** match, proving end-to-end
    context propagation through the Kafka headers. If a Jaeger v2 instance
    is reachable at `http://localhost:4318` (the default OTLP exporter
    endpoint), the same trace appears in Jaeger's UI as a producer span
    with a child consumer span.

The plan is deliberately scoped to interpreter-level instrumentation. It does
not add a new effect operation, does not change the existing
`Kafka.Effectful.{Producer,Consumer}.Effect` GADTs, and does not add a
"tracing-aware" facade to `Kafka.Effectful` — the new modules sit beside the
existing ones and are reached by direct import. This matches the structure
the masterplan envisioned ("traced interpreter variants") and avoids
forcing every existing user to learn a new operation surface.


## Progress

- [x] Milestone 1: Add the cabal dependency edges and create empty module
      stubs so the rest of the work has a place to land. The library
      builds with the new `hs-opentelemetry-api` and
      `hs-opentelemetry-semantic-conventions` build-depends but does not
      yet expose any tracing behavior. (Done 2026-05-06.)
- [x] Milestone 2: Implement `Kafka.Effectful.OpenTelemetry.Semantic`
      (pure attribute helpers). (Done 2026-05-06.)
- [x] Milestone 3: Implement `Kafka.Effectful.OpenTelemetry.Propagation`
      (header bridges between `hw-kafka-client.Headers` and
      `Network.HTTP.Types.RequestHeaders`). (Done 2026-05-06.)
- [x] Milestone 4: Implement `runKafkaProducerTraced` in
      `Kafka.Effectful.OpenTelemetry.Producer.Interpreter`. (Done
      2026-05-06.)
- [x] Milestone 5: Implement `runKafkaConsumerTraced` in
      `Kafka.Effectful.OpenTelemetry.Consumer.Interpreter`. (Done
      2026-05-06.)
- [x] Milestone 6: Add `Kafka.Effectful.OpenTelemetry` facade and wire all
      five modules into `kafka-effectful.cabal`'s `exposed-modules`.
      (Done 2026-05-06.)
- [x] Milestone 7: Add the `kafka-effectful-test` test-suite that proves
      attribute correctness, propagation round-trip, and shibuya
      compatibility (matching attribute keys and value types). All 23
      tests pass. (Done 2026-05-06.)
- [x] Milestone 8: Add the `example-otel-tracing` executable behind the
      existing `examples` flag, gated on a reachable broker. (Done
      2026-05-06; builds cleanly under `-f examples`. End-to-end run
      against a live broker is the user-facing acceptance step.)
- [x] Milestone 9: Update `README.md` and `CHANGELOG.md`. Bump the version
      to `0.2.0.0` (additive minor change post-0.1). Version bump done
      in Milestone 1; README "OpenTelemetry tracing" section and
      CHANGELOG entry done now. (Done 2026-05-06.)
- [x] Milestone 10: Outcomes & Retrospective. (Done 2026-05-06.)


## Surprises & Discoveries

- 2026-05-06 (Milestone 1): The plan placeholder bounds for the
  test-suite OTel deps undershot the actual published versions. On
  Hackage today, `hs-opentelemetry-sdk` is at `0.1.0.1` (not `0.0`)
  and `hs-opentelemetry-exporter-otlp` is at `0.1.0.0` (not `0.0`).
  Adjusted the cabal pins to `^>=0.1` for both. The
  `hs-opentelemetry-exporter-in-memory` pin matches at `^>=0.0.1`
  (latest is `0.0.1.4`). Library/test/example builds resolve cleanly.

- 2026-05-06 (Milestone 1): Added `unordered-containers` to both the
  library and the test-suite. The plan did not call this out, but
  `OpenTelemetry.Attributes.Map.AttributeMap` is
  `Data.HashMap.Strict.HashMap Text Attribute`, and we need to be able
  to inspect / construct that hashmap from outside the OTel package
  (e.g. in tests that compare against
  `shibuya-kafka-adapter`\'s `HashMap`-typed `kafkaSpanAttributes`).

- 2026-05-06 (Milestone 7): The `hs-opentelemetry-sdk` batch span
  processor requires GHC's threaded runtime — `cabal test` without
  `-threaded` raises `"The hs-opentelemetry batch processor does not
  work without the -threaded GHC flag!"` even when no spans are
  emitted, because `initializeGlobalTracerProvider` calls
  `batchProcessor` during construction. Added `ghc-options:
  -threaded` to the `kafka-effectful-test` cabal stanza. The
  example executable will need the same.


## Decision Log

- Decision: Implement OTel support as two new interpreter variants
  (`runKafkaProducerTraced`, `runKafkaConsumerTraced`) that wrap the
  existing `KafkaProducer` / `KafkaConsumer` effects, rather than as new
  effect operations.
  Rationale: Keeps the effect surface unchanged, so existing user code is
  untouched. Matches the masterplan's stated post-0.1 plan ("traced
  interpreter variants"). Mirrors the upstream
  `OpenTelemetry.Instrumentation.Kafka` design (free functions that wrap
  the underlying produce/poll calls). Users who do not want OTel pay
  nothing — they keep importing `Kafka.Effectful` and never reach the new
  module tree.
  Date: 2026-05-06

- Decision: Place the OTel work in the same `kafka-effectful` library
  rather than spinning a separate `kafka-effectful-opentelemetry` package.
  Rationale: The `hs-opentelemetry-api` and
  `hs-opentelemetry-semantic-conventions` build-depends are small (the
  same edges `shibuya-kafka-adapter` already takes directly). A separate
  package would add a release-coordination tax for an experimental
  library that is still on `0.x`. If the dep weight ever becomes
  problematic, the modules can be carved out into a sub-package later
  without breaking import paths in user code.
  Date: 2026-05-06

- Decision: Use the upstream
  `OpenTelemetry.SemanticConventions.{messaging_system,
  messaging_destination_name, messaging_operation,
  messaging_kafka_destination_partition, messaging_kafka_message_offset,
  messaging_kafka_message_key}` keys verbatim, and produce the exact
  same key strings and value types that
  `shibuya-kafka-adapter/src/Shibuya/Adapter/Kafka/Convert.hs`'s
  `kafkaSpanAttributes` function uses
  (`messaging.kafka.destination.partition` as `Int64`,
  `messaging.kafka.message.offset` as `Int64`, `messaging.system` as
  `Text` `"kafka"`).
  Rationale: "Compatible" means a user who is layering
  `kafka-effectful` and `shibuya-kafka-adapter` does not see any
  attribute key drift. Pinning both projects to the same
  `hs-opentelemetry-semantic-conventions` keys guarantees this — if
  upstream renames a key in a future release, both libraries break
  together (which is the desired failure mode), instead of silently
  emitting different keys.
  Date: 2026-05-06

- Decision: Span name is `"<operation> <topic>"` per the OTel messaging
  semantic conventions ("send orders", "process orders"), matching what
  `shibuya-kafka-adapter`'s framework span uses and what the upstream
  `OpenTelemetry.Instrumentation.Kafka` produces.
  Rationale: This is the spec-aligned span name shape. Diverging would
  break Jaeger / Grafana dashboards built around the convention.
  Date: 2026-05-06

- Decision: For `pollMessage`, do not open a span when the underlying
  call returns `Nothing` (timeout). Open a span only on a successful
  record return.
  Rationale: A timeout is not a "process this message" event; opening a
  span there would emit a noisy zero-attribute span every poll cycle and
  would not have a meaningful trace parent. The existing
  timeout-returns-`Nothing` semantics from
  `docs/plans/2-fix-poll-message-timeout-semantics.md` are preserved by
  this choice.
  Date: 2026-05-06

- Decision: For `pollMessageBatch`, open one Consumer-kind span per
  successfully-decoded record (one per `Right cr`), and skip the
  `Left err` entries (those are fatal poll errors that the caller will
  classify and re-throw, not message-processing events).
  Rationale: Each successful record represents a distinct unit of work
  for the application. Per-record spans match how downstream consumers
  structure their handlers and what `shibuya-kafka-adapter`'s framework
  per-message span does today.
  Date: 2026-05-06


## Outcomes & Retrospective

What was achieved (2026-05-06):

- Five new exposed modules under `Kafka.Effectful.OpenTelemetry.*`:
  the `Semantic` attribute-builder helpers, the `Propagation`
  trace-context header bridges, the `Producer.Interpreter`
  (`runKafkaProducerTraced`), the `Consumer.Interpreter`
  (`runKafkaConsumerTraced`), and the single-import facade.
- A new `kafka-effectful-test` test-suite with 23 tasty-hunit cases
  covering attribute correctness, propagation round-trip, and a
  shibuya-compatibility pin. Passes under
  `cabal test --test-show-details=streaming`.
- A new `example-otel-tracing` executable behind the existing
  `examples` flag. Builds cleanly under `-f examples`; the
  end-to-end run against a live broker (CLI step in Concrete Steps)
  is the user-facing acceptance gate.
- Version bumped from `0.1.0.0` to `0.2.0.0`. README has a new
  "OpenTelemetry tracing" section with a 25-line snippet that wires
  `runKafkaProducerTraced` and a Compatibility paragraph for the
  shibuya layering case. CHANGELOG records the change under
  "Unreleased".
- The existing `Kafka.Effectful.{Producer,Consumer}.*` modules were
  not modified at the source level. Users who do not import the new
  modules see no behavior change.

What remains:

- `messaging.message.id`. Kafka has no native message ID, so the
  conventional value is `<topic>-<partition>-<offset>`. This plan
  did not synthesize that attribute — `shibuya-kafka-adapter` does,
  via its `mkMessageId` helper. A future plan could extend
  `consumerRecordAttributes` to emit it for consumer spans.
- The `example-otel-tracing` end-to-end run against a live broker
  is documented but has not been executed in CI. Validating the
  trace-ID match on a real Kafka installation is a manual user
  step.

Lessons learned:

- The plan placeholder version pins for the test-suite OTel deps
  (`hs-opentelemetry-sdk ^>=0.0`, `hs-opentelemetry-exporter-otlp
  ^>=0.0`) undershot what is actually published on Hackage. We
  bumped them to `^>=0.1` after consulting the local source tree.
  Worth noting in the future: when calling for "look up the actual
  pin", run `cabal info <package>` and use the latest minor that
  matches the source on disk.
- The `hs-opentelemetry-sdk` batch span processor requires GHC's
  threaded runtime. Without `-threaded` the test-suite *and* the
  example program fail at `initializeGlobalTracerProvider` with
  the runtime error
  @"The hs-opentelemetry batch processor does not work without the
  -threaded GHC flag!"@ — even when no spans are emitted. This is
  not documented in the SDK README; we discovered it the first
  time `cabal test` ran.
- `treefmt` (the project's pre-commit formatter) reformats cabal
  alignment, so the cabal file's column layout sometimes shifts on
  commit. Anticipating this saved one round of `git commit` retry.


## Context and Orientation

This section names every file, module, and external reference the
implementer needs. Read it once before starting; it does not assume
familiarity with this codebase.

### What kafka-effectful is today

`kafka-effectful` is a single Haskell library at the repository root.
The cabal file is `kafka-effectful.cabal`. The library exposes seven
modules under `src/`:

    src/Kafka/Effectful.hs                         -- combined facade
    src/Kafka/Effectful/Producer.hs                -- producer facade
    src/Kafka/Effectful/Producer/Effect.hs         -- KafkaProducer GADT
    src/Kafka/Effectful/Producer/Interpreter.hs    -- runKafkaProducer
    src/Kafka/Effectful/Producer/Transaction.hs    -- cross-effect helper
    src/Kafka/Effectful/Consumer.hs                -- consumer facade
    src/Kafka/Effectful/Consumer/Effect.hs         -- KafkaConsumer GADT
    src/Kafka/Effectful/Consumer/Interpreter.hs    -- runKafkaConsumer

The library has **no test-suite** today (the current `kafka-effectful.cabal`
has only `library` and two example `executable` stanzas). This plan
adds a test-suite.

The two effects and their operations, all dynamic-dispatch GADTs, are:

`KafkaProducer` (in `Kafka.Effectful.Producer.Effect`) carries
`ProduceMessage`, `ProduceMessage'`, `ProduceMessageSync`,
`ProduceMessageBatch`, `FlushProducer`, `InitTransactions`,
`BeginTransaction`, `CommitTransaction`, `AbortTransaction`,
`SendOffsetsToTransaction`, and `AskProducerHandle`. Each constructor
ships its arguments as plain hw-kafka-client types
(`ProducerRecord`, `Timeout`, etc.).

`KafkaConsumer` (in `Kafka.Effectful.Consumer.Effect`) carries
`PollMessage`, `PollMessageBatch`, `CommitOffsetMessage`,
`CommitAllOffsets`, `CommitPartitionsOffsets`, `StoreOffsets`,
`StoreOffsetMessage`, `Assign`, `PausePartitions`, `ResumePartitions`,
`SeekPartitions`, `Committed`, `Position`, `Assignment`, `Subscription`,
and `AskConsumerHandle`.

The two existing interpreters (`Kafka.Effectful.Producer.Interpreter.runKafkaProducer`
and `Kafka.Effectful.Consumer.Interpreter.runKafkaConsumer`) follow the
same pattern: `Exception.bracket` to acquire and release a
`hw-kafka-client` handle, and then `interpret (handleProducer producer)
action` (or `handleConsumer consumer`) to dispatch each effect operation
to the underlying call. Read the existing handler code in
`src/Kafka/Effectful/Producer/Interpreter.hs:48` and
`src/Kafka/Effectful/Consumer/Interpreter.hs:62` before writing the new
traced variants — the new variants follow exactly the same shape.

The library currently depends on `effectful-core >=2.5 && <2.7`,
`hw-kafka-client >=5.3 && <6`, `bytestring >=0.11 && <0.13`, `containers
>=0.6 && <0.8`, `text >=2.0 && <2.2`, and `base >=4.21 && <5`.

### What "OpenTelemetry compatible with shibuya-kafka-adapter" means

`shibuya-kafka-adapter` is a sibling project at
`/Users/shinzui/Keikaku/bokuno/shibuya-project/shibuya-kafka-adapter`. It
imports `kafka-effectful` (specifically `Kafka.Effectful.Consumer.Effect`)
and adds OTel-aware envelope construction in its own
`shibuya-kafka-adapter/src/Shibuya/Adapter/Kafka/Convert.hs`. That file
is the canonical reference for the semantic conventions this library
must align with. Read it before writing the new helpers; it is short
(125 lines).

The two OTel-relevant functions in `Convert.hs` are:

`extractTraceHeaders :: Headers -> Maybe TraceHeaders` — looks for
`traceparent` and `tracestate` keys in the message headers. Returns
`Nothing` when no `traceparent` is present (a `tracestate` alone is
not a valid context).

`kafkaSpanAttributes :: PartitionId -> Offset -> HashMap Text Attribute`
— produces the three spec-aligned attribute pairs:

    ("messaging.system",                          toAttribute ("kafka" :: Text))
    (unkey Sem.messaging_kafka_destination_partition, toAttribute (fromIntegral pid :: Int64))
    (unkey Sem.messaging_kafka_message_offset,        toAttribute (off :: Int64))

`unkey` here is `OpenTelemetry.Attributes.unkey`, which strips the
phantom-typed `AttributeKey a` wrapper and returns the raw `Text` key
name. `Sem.messaging_kafka_destination_partition` and
`Sem.messaging_kafka_message_offset` come from
`OpenTelemetry.SemanticConventions`. The traced interpreters in this
plan **must** produce the same three keys with the same value types,
plus the additional spec-recommended keys
(`messaging.destination.name`, `messaging.operation`,
`messaging.kafka.message.key` when decodable) that the upstream
`OpenTelemetry.Instrumentation.Kafka` reference adds.

The shibuya-side flow is "extract trace context → put it in
`Envelope.traceContext` → let the framework's per-message processor
open the Consumer-kind span using the extracted context as the parent."
This means **kafka-effectful's traced consumer interpreter does the
extraction and the span-opening together** — the user's handler runs
inside the span, and the message's incoming `traceparent` becomes the
span's parent. A user who later layers `shibuya-kafka-adapter` on top
of `runKafkaConsumerTraced` would end up with two spans per message
(the kafka-effectful poll span and the shibuya per-message span, both
linked through context). That stacking is documented under the README's
new "OpenTelemetry" section as "do not stack the two unless you want
a Receive→Process span split, which is the spec-compliant shape" — see
Milestone 9.

### Reference implementation: hs-opentelemetry-instrumentation-hw-kafka-client

The upstream library `hs-opentelemetry-instrumentation-hw-kafka-client`
lives at
`/Users/shinzui/Keikaku/hub/haskell/hs-opentelemetry-project/hs-opentelemetry/instrumentation/hw-kafka-client`.
Its single module `OpenTelemetry.Instrumentation.Kafka` is 243 lines
and is the closest analogue to what this plan adds. Read it end-to-end
before writing the new interpreters; the design parallels are direct.

Notable bits to lift from the upstream module:

- `producerSpanArgs` and `consumerSpanArgs` set `kind = Producer` and
  `kind = Consumer` on the upstream `defaultSpanArguments`. Use the same.

- `producerOperationName = "send"` and `consumerOperationName = "process"`
  build the spec-aligned span name as `"<operation> <topic>"` via
  `producerOperationName <> " " <> unTopicName topicName`.

- `producerAttributes :: ProducerRecord -> AttributeMap` and
  `consumerAttributes :: ConsumerProperties -> ConsumerRecord ... -> AttributeMap`
  show the exact attribute set the new helpers must produce. Note that
  the upstream `consumerAttributes` reads the consumer group from
  `cpProps consumerProperties`'s `"group.id"` map entry; the new
  `consumerRecordAttributes` in this plan does **not** add the
  consumer-group attribute (we do not have access to the
  `ConsumerProperties` at attribute-build time, since the helper is
  pure and takes only the record). The traced consumer interpreter
  *does* have the properties, so it can add `messaging.kafka.consumer.group`
  itself when wrapping the helper. This is documented under the
  Interfaces and Dependencies section below.

- `kafkaHeadersToHttpHeaders` and `httpHeadersToKafkaHeaders` bridge
  `hw-kafka-client`'s `Headers` (a list of `(ByteString, ByteString)`)
  to `Network.HTTP.Types.RequestHeaders` (a list of `(CI ByteString,
  ByteString)`). The new
  `Kafka.Effectful.OpenTelemetry.Propagation` module provides this same
  bridge.

- `produceMessage` (the upstream traced producer) calls
  `inSpan'' tracer spanName spanArguments $ \newSpan -> ...` with the
  attributes pre-baked into `spanArguments` via
  `addAttributesToSpanArguments`, and inside the span uses
  `inject propagator (Context.insertSpan newSpan ctxt) []` to pull the
  W3C headers out as a `RequestHeaders`, then merges them onto the
  record's existing `prHeaders` and finally calls
  `KP.produceMessage producer newKafkaRecord`.

- `pollMessage` (the upstream traced consumer) calls
  `KC.pollMessage consumer timeout` first (so the span is opened only
  on success), then `extract propagator (kafkaHeadersToHttpHeaders ...) ctxt`
  to get the inbound context, `attachContext ctx` to make it the
  current thread context, and finally `inSpan''` to open the consumer
  span. The `inSpan''` callback returns `Right cr` so the span lifetime
  ends when the consumer span block ends.

The new traced interpreters do not import or call the upstream module.
They duplicate the design (with attribution in module haddocks) so the
new code remains lifted into the `effectful` ecosystem rather than
forcing users to manage `MonadUnliftIO m` constraints alongside
`Eff es a`.

### OpenTelemetry semantic conventions for messaging

The library
`hs-opentelemetry-semantic-conventions` ships at
`/Users/shinzui/Keikaku/hub/haskell/hs-opentelemetry-project/hs-opentelemetry/semantic-conventions`.
Its single module `OpenTelemetry.SemanticConventions` exports
strongly-typed `AttributeKey` values for every key in the upstream
specification v1.24. The exact symbols this plan uses are:

    messaging_system                       :: AttributeKey Text     -- "messaging.system"
    messaging_destination_name             :: AttributeKey Text     -- "messaging.destination.name"
    messaging_operation                    :: AttributeKey Text     -- "messaging.operation"
    messaging_message_id                   :: AttributeKey Text     -- "messaging.message.id"
    messaging_kafka_consumer_group         :: AttributeKey Text     -- "messaging.kafka.consumer.group"
    messaging_kafka_destination_partition  :: AttributeKey Int64    -- "messaging.kafka.destination.partition"
    messaging_kafka_message_offset         :: AttributeKey Int64    -- "messaging.kafka.message.offset"
    messaging_kafka_message_key            :: AttributeKey Text     -- "messaging.kafka.message.key"

The phantom-typed `AttributeKey a` is from `OpenTelemetry.Attributes.Key`.
`OpenTelemetry.Attributes.Map.insertAttributeByKey` takes an
`AttributeKey a` plus a value of type `a` and updates an `AttributeMap`.
The pure helpers in `Kafka.Effectful.OpenTelemetry.Semantic` use this
to compose attribute maps without having to stringify keys by hand.

The W3C trace context propagator is provided by
`hs-opentelemetry-propagator-w3c`, which the application's tracer
provider already wires (see the Shibuya jitsurei example). The traced
interpreters fetch the propagator via
`getTracerProviderPropagators <$> getGlobalTracerProvider` — the same
indirection the upstream module uses.

### How `Eff es a` interpreters integrate with `inSpan''`

`inSpan''` from `OpenTelemetry.Trace.Core` has type:

    inSpan''
      :: (MonadUnliftIO m, HasCallStack)
      => Tracer
      -> Text
      -> SpanArguments
      -> (Span -> m a)
      -> m a

`Eff es` has a `MonadUnliftIO` instance when `IOE :> es`, exposed by
`effectful` itself. The traced interpreters require `IOE :> es` (the
existing untraced ones already do), so `inSpan''` lifts directly into
`Eff es`. Inside the `interpret` handler, each operation case wraps
its existing `Effectful.liftIO`-based body with an `inSpan''` block —
no `MonadUnliftIO` boilerplate is needed.

### Files that will be created or modified

New files:

    src/Kafka/Effectful/OpenTelemetry.hs                         -- facade
    src/Kafka/Effectful/OpenTelemetry/Semantic.hs                -- attribute helpers
    src/Kafka/Effectful/OpenTelemetry/Propagation.hs             -- header bridges
    src/Kafka/Effectful/OpenTelemetry/Producer/Interpreter.hs    -- runKafkaProducerTraced
    src/Kafka/Effectful/OpenTelemetry/Consumer/Interpreter.hs    -- runKafkaConsumerTraced
    test/Main.hs                                                 -- test-suite entry
    test/Kafka/Effectful/OpenTelemetry/SemanticTest.hs           -- attribute helper tests
    test/Kafka/Effectful/OpenTelemetry/PropagationTest.hs        -- header round-trip tests
    test/Kafka/Effectful/OpenTelemetry/ShibuyaCompatibilityTest.hs -- key/type pin
    examples/OtelTracing.hs                                      -- end-to-end demo

Modified files:

    kafka-effectful.cabal           -- new exposed-modules, build-depends, test-suite, executable
    README.md                       -- "OpenTelemetry" section
    CHANGELOG.md                    -- "Unreleased" entry, version bump rationale

Total: 9 new files, 3 modified files. The existing `src/` modules
under `Kafka.Effectful.{Producer,Consumer}` are not touched — the
traced interpreters wrap the existing effects rather than rewriting
them.


## Plan of Work

The work is organized into ten milestones. The first six are additive
implementation work, the seventh is testing, the eighth is a
runnable example, and the last two are documentation and retrospective.
Each milestone leaves the codebase in a buildable state.

### Milestone 1 — Cabal wiring and module skeletons

This milestone makes the build accept the new dependency edges and the
new module paths, so subsequent milestones can fill in module bodies
without touching the cabal file again. At the end of this milestone,
`cabal build all` succeeds; the new modules exist as empty stubs
exporting nothing.

Edit `kafka-effectful.cabal`:

- Bump `version: 0.1.0.0` to `version: 0.2.0.0` (additive minor bump
  per Haskell PVP — new modules and a new dependency, no breaking
  changes to existing modules).

- Under `library` → `build-depends`, add three new edges (alphabetized
  between the existing entries):

      , case-insensitive                       >=1.2  && <1.3
      , hs-opentelemetry-api                   ^>=0.3
      , hs-opentelemetry-semantic-conventions  ^>=0.1
      , http-types                             >=0.12 && <0.13
      , unliftio-core                          >=0.2  && <0.3

  The bounds are the same that `shibuya-kafka-adapter`'s cabal file
  uses, except for `case-insensitive`/`http-types`/`unliftio-core`,
  whose bounds match the upstream
  `hs-opentelemetry-instrumentation-hw-kafka-client` package.

- Under `library` → `exposed-modules`, add five new entries
  (alphabetized after the existing `Kafka.Effectful.Producer.Transaction`):

      Kafka.Effectful.OpenTelemetry
      Kafka.Effectful.OpenTelemetry.Consumer.Interpreter
      Kafka.Effectful.OpenTelemetry.Producer.Interpreter
      Kafka.Effectful.OpenTelemetry.Propagation
      Kafka.Effectful.OpenTelemetry.Semantic

- Add a new `test-suite kafka-effectful-test` stanza after the existing
  executables. Use the same `common warnings` import the library uses,
  default-language `GHC2024`, and `type: exitcode-stdio-1.0`. Test
  build-depends:

      , base                                   >=4.21 && <5
      , bytestring
      , containers
      , effectful-core
      , hs-opentelemetry-api                   ^>=0.3
      , hs-opentelemetry-exporter-in-memory    ^>=0.0
      , hs-opentelemetry-sdk                   ^>=0.0
      , hs-opentelemetry-semantic-conventions  ^>=0.1
      , http-types
      , hw-kafka-client
      , kafka-effectful
      , tasty                                  ^>=1.5
      , tasty-hunit                            ^>=0.10
      , text
      , unordered-containers

  Hs-source-dirs: `test`. Main-is: `Main.hs`. Other-modules:

      Kafka.Effectful.OpenTelemetry.PropagationTest
      Kafka.Effectful.OpenTelemetry.SemanticTest
      Kafka.Effectful.OpenTelemetry.ShibuyaCompatibilityTest

  Look up the actual `^>=` pin for `hs-opentelemetry-exporter-in-memory`
  and `hs-opentelemetry-sdk` by reading the cabal files at
  `/Users/shinzui/Keikaku/hub/haskell/hs-opentelemetry-project/hs-opentelemetry/exporters/in-memory/`
  and `.../sdk/` before writing this stanza. If the bounds are
  different from the placeholder above, use what the source declares.

- Add an `executable example-otel-tracing` stanza after the existing
  `executable example-transactional-etl`. Mirror the existing example
  stanzas: gated behind `if !flag(examples) buildable: False`,
  hs-source-dirs `examples`, main-is `OtelTracing.hs`. Build-depends
  must include `hs-opentelemetry-api`,
  `hs-opentelemetry-exporter-otlp`, `hs-opentelemetry-sdk`, the new
  `kafka-effectful`, plus the same base/bytestring/text/effectful-core
  set the existing examples use.

Create five empty-but-compilable module stubs. Each stub declares its
module header with a brief one-paragraph haddock, an empty export
list, and any `import` lines the haddock references. The bodies are
filled in by Milestones 2–6.

Run `cabal build all`. Expect success; if cabal cannot resolve the new
deps, capture the error in Surprises & Discoveries and adjust the
bounds.

Acceptance: `cabal build` exits zero. `cabal sdist --list-only` shows
the five new module files. `cabal repl` opens.

### Milestone 2 — Implement Kafka.Effectful.OpenTelemetry.Semantic

This module exports two pure functions that build the spec-aligned
attribute maps for a producer record and for a consumer record.

In `src/Kafka/Effectful/OpenTelemetry/Semantic.hs`, define:

    module Kafka.Effectful.OpenTelemetry.Semantic
      ( producerRecordAttributes
      , consumerRecordAttributes
      , kafkaMessagingSystem
      , producerOperationName
      , consumerOperationName
      , producerSpanName
      , consumerSpanName
      ) where

`kafkaMessagingSystem :: Text` is the constant `"kafka"` — the value
the `messaging.system` attribute carries. Exporting it as a named
constant lets tests assert against the same source of truth.

`producerOperationName :: Text` and `consumerOperationName :: Text`
are `"send"` and `"process"` respectively (as in the upstream
reference).

`producerSpanName :: TopicName -> Text` and
`consumerSpanName :: TopicName -> Text` build the canonical span name
`"<operation> <topic>"`. The upstream reference uses string
concatenation; this plan does the same:

    producerSpanName (TopicName t) = producerOperationName <> " " <> t
    consumerSpanName (TopicName t) = consumerOperationName <> " " <> t

`producerRecordAttributes :: ProducerRecord -> AttributeMap` produces:

- `messaging.system = "kafka"`
- `messaging.destination.name = unTopicName (prTopic record)`
- `messaging.operation = "send"`
- `messaging.kafka.destination.partition = <Int64>` only when
  `prPartition` is `SpecifiedPartition n`. (The upstream reference
  omits this attribute for `UnassignedPartition`, since the broker
  picks the partition.)
- `messaging.kafka.message.key = <Text>` only when `prKey` is
  `Just bs` and `decodeUtf8' bs` returns `Right text`. (The upstream
  reference uses the same `decodeUtf8' . rightToMaybe` shape; this
  plan reuses that exact predicate.)

The function builds an `AttributeMap` (which is
`HashMap Text Attribute`) by composing
`OpenTelemetry.Attributes.Map.insertAttributeByKey` calls starting
from `mempty`. Use the `messaging_system`,
`messaging_destination_name`, `messaging_operation`,
`messaging_kafka_destination_partition`, and
`messaging_kafka_message_key` `AttributeKey` values from
`OpenTelemetry.SemanticConventions`.

`consumerRecordAttributes :: ConsumerRecord (Maybe ByteString) (Maybe ByteString) -> AttributeMap`
produces:

- `messaging.system = "kafka"`
- `messaging.destination.name = unTopicName (crTopic record)`
- `messaging.operation = "process"`
- `messaging.kafka.destination.partition = <Int64>` from
  `unPartitionId (crPartition record)`. Always present (a polled
  record always has a known partition).
- `messaging.kafka.message.offset = <Int64>` from
  `unOffset (crOffset record)`. Always present.
- `messaging.kafka.message.key = <Text>` only when `crKey` is
  `Just bs` and decodes as UTF-8.

Note that this helper does **not** emit `messaging.kafka.consumer.group`.
That attribute requires the `ConsumerProperties`, which a pure
record-only helper does not have access to. The traced consumer
interpreter (Milestone 5) adds the consumer-group attribute itself,
on top of what this helper returns.

Acceptance: `cabal build` is clean. `cabal repl` can call
`producerRecordAttributes` and `consumerRecordAttributes` against
synthetic records and the resulting maps include the expected keys
(verified manually in REPL; the structured tests come in Milestone 7).

### Milestone 3 — Implement Kafka.Effectful.OpenTelemetry.Propagation

This module exports the bridges between `hw-kafka-client`'s
`Kafka.Types.Headers` (a list of `(ByteString, ByteString)`) and
`Network.HTTP.Types.RequestHeaders` (a list of
`(CI ByteString, ByteString)`), plus convenience helpers that fetch
the global propagator and inject/extract context in one call.

In `src/Kafka/Effectful/OpenTelemetry/Propagation.hs`, define:

    module Kafka.Effectful.OpenTelemetry.Propagation
      ( -- * Header bridges
        kafkaHeadersToRequestHeaders
      , requestHeadersToKafkaHeaders
        -- * Convenience: round-trip W3C trace context
      , extractTraceContextFromRecord
      , injectTraceContextIntoRecord
      ) where

`kafkaHeadersToRequestHeaders :: Headers -> RequestHeaders` maps each
`(bsKey, bsVal)` to `(CI.mk bsKey, bsVal)`. The upstream reference
uses `headersToList` and `Data.Bifunctor.first CI.mk`; this plan does
the same.

`requestHeadersToKafkaHeaders :: RequestHeaders -> Headers` maps each
`(ciKey, bsVal)` to `(CI.foldedCase ciKey, bsVal)` and rebuilds via
`headersFromList`.

`extractTraceContextFromRecord :: ConsumerRecord k v -> Context -> IO Context`
takes a `ConsumerRecord`, fetches the global tracer provider's
propagator, and calls `extract propagator (kafkaHeadersToRequestHeaders
(crHeaders cr)) ctx`. Returns the augmented `Context`. The traced
consumer interpreter uses this to obtain the inbound context before
opening the per-record span.

`injectTraceContextIntoRecord :: Context -> ProducerRecord -> IO ProducerRecord`
takes a `Context` and a `ProducerRecord`, fetches the global
propagator, calls
`inject propagator ctx []` to get the outbound `RequestHeaders`,
converts those to `Headers`, merges them onto the record's existing
`prHeaders`, and returns the augmented record. The traced producer
interpreter uses this to inject the current span context into the
record before sending.

Acceptance: round-trip property — for any input `Headers` `hs` whose
keys are valid HTTP header names,
`requestHeadersToKafkaHeaders (kafkaHeadersToRequestHeaders hs)` is
case-fold-equivalent to `hs`. Verified in Milestone 7's
`PropagationTest`.

### Milestone 4 — Implement runKafkaProducerTraced

In `src/Kafka/Effectful/OpenTelemetry/Producer/Interpreter.hs`,
define:

    runKafkaProducerTraced ::
      (IOE :> es, Error KafkaError :> es) =>
      Tracer ->
      ProducerProperties ->
      Eff (KafkaProducer : es) a ->
      Eff es a

The implementation mirrors `runKafkaProducer` from
`Kafka.Effectful.Producer.Interpreter`:
`Exception.bracket` to acquire/release the producer handle, then
`interpret (handleTracedProducer tracer producer) action`.

The handler `handleTracedProducer` differs from the existing
`handleProducer` only in the way it dispatches the four
"send a record" operations:

- `ProduceMessage record`: open a span, inject context into the
  record's headers, then call the underlying
  `K.produceMessage producer instrumentedRecord`, throw on `Just err`.
- `ProduceMessage' record cb`: same — open span, inject, call
  `K.produceMessage' producer instrumentedRecord cb`, throw on
  `Left ImmediateError`.
- `ProduceMessageSync record`: same — open span, inject, call the
  sync helper, throw on `Left ImmediateError` or
  `DeliveryFailure/NoMessageError`.
- `ProduceMessageBatch records`: open one span per record (or one
  span for the batch — see Decision Log below), inject context into
  each record, call the inlined `mapM` over `K.produceMessage`.

Decision: `ProduceMessageBatch` opens **one span per record**, not
one span per batch call. Rationale: the upstream
`OpenTelemetry.Instrumentation.Kafka.produceMessage` opens one span
per record; the per-record granularity is what the spec recommends
("a separate Producer span for each message"). A batch-level span
would lose per-record `messaging.kafka.message.key` and partition
attributes. Recording this as an in-plan decision because it is a
design choice the implementer should not have to re-derive.

The remaining ten `KafkaProducer` operations
(`FlushProducer`, `InitTransactions`, `BeginTransaction`,
`CommitTransaction`, `AbortTransaction`,
`SendOffsetsToTransaction`, `AskProducerHandle`) are passed through
unchanged — they do not represent message sends and do not get a
span.

The span body uses `inSpan'' tracer (producerSpanName (prTopic record))
(addAttributesToSpanArguments (producerRecordAttributes record)
producerSpanArgs) $ \newSpan -> do { ctx <- getContext; ctxWithSpan <- ...; instrumentedRecord <- liftIO (injectTraceContextIntoRecord (Context.insertSpan newSpan ctx) record); ... }`,
where `producerSpanArgs = defaultSpanArguments { kind = Producer }`.

Acceptance: `cabal build` is clean. The interpreter type-checks
against the existing `KafkaProducer` GADT. No behavior change for
the non-traced path is required (this is a new function alongside
`runKafkaProducer`).

### Milestone 5 — Implement runKafkaConsumerTraced

In `src/Kafka/Effectful/OpenTelemetry/Consumer/Interpreter.hs`,
define:

    runKafkaConsumerTraced ::
      (IOE :> es, Error KafkaError :> es) =>
      Tracer ->
      ConsumerProperties ->
      Subscription ->
      Eff (KafkaConsumer : es) a ->
      Eff es a

The implementation mirrors `runKafkaConsumer` from
`Kafka.Effectful.Consumer.Interpreter`. The handler differs only in
the way it dispatches the two "polling" operations:

- `PollMessage timeout`: call `K.pollMessage consumer timeout` first.
  On `Left (KafkaResponseError RdKafkaRespErrTimedOut)` return
  `Nothing` without opening a span (the timeout-returns-`Nothing`
  semantics established by
  `docs/plans/2-fix-poll-message-timeout-semantics.md` are
  preserved). On `Left err` rethrow without a span. On `Right cr`,
  extract the trace context from the record's headers, attach it as
  the current context, then open a Consumer-kind span named
  `consumerSpanName (crTopic cr)` with attributes built from
  `consumerRecordAttributes cr` plus the consumer-group attribute
  (added by the interpreter using
  `Map.lookup "group.id" (cpProps consumerProps)`). The span body
  returns `Just cr`.

- `PollMessageBatch timeout batchSize`: call
  `K.pollMessageBatch consumer timeout batchSize`; for each `Right cr`
  in the resulting list, open one span as in `PollMessage`. The
  resulting list's structure (positions of `Left err`s preserved) is
  unchanged. The spans are opened sequentially as the list is
  traversed — opening them in parallel would be incorrect because
  `attachContext` mutates a thread-local `Context`.

The remaining 14 `KafkaConsumer` operations are passed through
unchanged.

Acceptance: `cabal build` is clean. The interpreter type-checks
against the existing `KafkaConsumer` GADT. Manual smoke check via
`cabal repl` (constructing a fake consumer or a unit test using the
in-memory exporter) confirms a span is opened on a successful
synthetic poll.

### Milestone 6 — Facade module

In `src/Kafka/Effectful/OpenTelemetry.hs`, re-export the public
surface of the four implementation modules so the user only needs
one import to wire OTel:

    module Kafka.Effectful.OpenTelemetry
      ( -- * Traced interpreters
        runKafkaProducerTraced
      , runKafkaConsumerTraced
        -- * Pure attribute helpers
      , producerRecordAttributes
      , consumerRecordAttributes
      , producerSpanName
      , consumerSpanName
        -- * Trace-context propagation
      , extractTraceContextFromRecord
      , injectTraceContextIntoRecord
      , kafkaHeadersToRequestHeaders
      , requestHeadersToKafkaHeaders
      ) where

The facade does **not** re-export `Tracer`, `Span`, etc. from the
upstream OTel package — users who want those import them directly
from `OpenTelemetry.Trace` / `OpenTelemetry.Trace.Core`. Re-exporting
upstream types from a wrapper library tends to drift; pinning by
import is cleaner.

Acceptance: `cabal build` is clean. `cabal haddock` produces docs for
the new module without errors.

### Milestone 7 — Test-suite

Add a `tasty`-driven test-suite under `test/`. The suite verifies
three things, each in its own module.

`test/Main.hs` is the test driver — assembles the three test trees
via `Test.Tasty.testGroup`.

`test/Kafka/Effectful/OpenTelemetry/SemanticTest.hs` constructs
synthetic `ProducerRecord` and `ConsumerRecord` values and asserts
that `producerRecordAttributes` and `consumerRecordAttributes`:

- include `messaging.system = "kafka"` (Text);
- include `messaging.destination.name` set to the topic name (Text);
- include `messaging.operation` set to `"send"` or `"process"`;
- for the producer, include `messaging.kafka.destination.partition`
  with value `42 :: Int64` when the input is
  `SpecifiedPartition 42`, and exclude it when the input is
  `UnassignedPartition`;
- for the consumer, include `messaging.kafka.destination.partition`
  and `messaging.kafka.message.offset` (both Int64) and the offset
  value matches the input;
- include `messaging.kafka.message.key = "<text>"` (Text) when the
  key bytes are valid UTF-8, and exclude it when the bytes are
  invalid UTF-8 (test with a known-invalid byte sequence such as
  `BS.pack [0xC3, 0x28]`).

`test/Kafka/Effectful/OpenTelemetry/PropagationTest.hs` verifies the
header bridges. Build a `Headers` value with a known
`traceparent: 00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01`,
round-trip it through `kafkaHeadersToRequestHeaders` then
`requestHeadersToKafkaHeaders`, and assert the result has the same
`traceparent` value (after CI normalization). Then go the other way:
build a `Context` containing a freshly-created span (use
`hs-opentelemetry-sdk` to build a real `TracerProvider` with the
in-memory exporter from `hs-opentelemetry-exporter-in-memory`),
inject it via `injectTraceContextIntoRecord` into a synthetic
`ProducerRecord`, and assert the resulting record's `prHeaders`
contains a `traceparent` whose trace-id field matches the span's
context. Finally, extract from a constructed `ConsumerRecord` and
assert the recovered `Context` carries a span whose `traceId` matches
the original.

`test/Kafka/Effectful/OpenTelemetry/ShibuyaCompatibilityTest.hs` is
the compatibility pin. It mirrors what
`shibuya-kafka-adapter/src/Shibuya/Adapter/Kafka/Convert.hs`'s
`kafkaSpanAttributes` produces and asserts the new
`consumerRecordAttributes` agrees on the three keys shibuya already
uses: `messaging.system`, `messaging.kafka.destination.partition`,
`messaging.kafka.message.offset`. The exact values shibuya uses come
from this snippet of `Convert.hs:78-89` (paraphrased here so the
plan is self-contained):

    [ ("messaging.system",                              toAttribute ("kafka" :: Text))
    , (unkey messaging_kafka_destination_partition,     toAttribute (fromIntegral pid :: Int64))
    , (unkey messaging_kafka_message_offset,            toAttribute (off :: Int64))
    ]

The compatibility test constructs a `ConsumerRecord` with
`crPartition = PartitionId 7` and `crOffset = Offset 42` and asserts
that the resulting `consumerRecordAttributes` map contains the same
three keys with the same value types and same numeric values. If
either project later changes a key, this test fails — which is the
desired failure mode (it forces a coordinated update).

Run the suite:

    cabal test --test-show-details=streaming

Acceptance: all tests pass. Verbose output names every assertion.

### Milestone 8 — End-to-end example

Add `examples/OtelTracing.hs`. The program:

1. Parses CLI args for `--bootstrap-servers` and `--topic`.
2. Initialises an OTel tracer provider via
   `OpenTelemetry.Trace.initializeGlobalTracerProvider` (which reads
   `OTEL_EXPORTER_OTLP_ENDPOINT` and other env vars), and creates a
   tracer named `"kafka-effectful-example"`.
3. Sends one record with key `"k1"` and value `"hello otel"` via
   `runKafkaProducerTraced tracer producerProps $ do produceMessage
   record; flushProducer`. Captures and prints the producer-side
   trace ID.
4. Polls one record via
   `runKafkaConsumerTraced tracer consumerProps subscription $ do
   pollMessage (Timeout 5000)` against the same topic. Captures and
   prints the consumer-side trace ID.
5. Prints `"trace IDs match"` when the two trace IDs are equal,
   `"trace IDs differ"` otherwise. Exit code 0 on match, 1 on
   divergence.
6. Calls `shutdownTracerProvider` so the OTLP exporter flushes any
   buffered spans.

The example program follows the structure of
`shibuya-kafka-adapter-jitsurei/app/OtelDemo.hs` (consumer side) and
`shibuya-kafka-adapter-jitsurei/app/OtelProducerDemo.hs` (producer
side), but is end-to-end on its own — it does not depend on Shibuya.

Document the manual run procedure in the example's haddock:

    -- Spin up Kafka + Jaeger v2 (any docker-compose is fine; the
    -- shibuya-kafka-adapter repo's just process-up works).
    -- Then:
    --   cabal run example-otel-tracing -f examples -- \
    --     --bootstrap-servers localhost:9092 \
    --     --topic otel-demo
    -- Expect: two trace IDs printed, both equal, exit code 0.

Acceptance: a manual run against a local Kafka broker prints two
matching trace IDs and exits zero. If a Jaeger v2 instance is
reachable at `http://localhost:4318`, the same trace appears with a
producer span and a child consumer span linked by trace ID.

### Milestone 9 — README and CHANGELOG

In `README.md`, add a new "OpenTelemetry tracing" section after the
existing "Consumer" / "Running it" subsections (and before "Module
Structure"). The section explains:

- How to swap the runner — replace `runKafkaProducer` with
  `runKafkaProducerTraced tracer`. Include a 10-line snippet that
  imports `Kafka.Effectful.OpenTelemetry`,
  `OpenTelemetry.Trace.initializeGlobalTracerProvider`, and shows
  the new wiring.
- The semantic-conventions guarantee: span names are `"send <topic>"`
  / `"process <topic>"` (Producer / Consumer kind), attributes match
  the upstream OTel messaging spec.
- Trace-context propagation through Kafka headers: the producer
  injects W3C `traceparent`/`tracestate` into the record's headers;
  the consumer extracts them on poll and uses them as the parent of
  the per-message span.
- A short "Compatibility" paragraph: the attribute keys this library
  emits agree with `shibuya-kafka-adapter`'s envelope-level
  attributes, so a user who layers both gets a Receive→Process span
  split (kafka-effectful's poll span as parent, shibuya's framework
  per-message span as child). If only one span per message is
  desired, use either kafka-effectful's traced runner *or* shibuya's
  framework span — not both.

Update the "Module Structure" table to include the five new modules.

In `CHANGELOG.md`, under the existing `## Unreleased` heading, add a
new bullet:

    - Add OpenTelemetry tracing support via opt-in interpreter variants
      `runKafkaProducerTraced` and `runKafkaConsumerTraced`. New
      modules under `Kafka.Effectful.OpenTelemetry.*` provide the
      attribute-builder helpers (`producerRecordAttributes`,
      `consumerRecordAttributes`) and the W3C trace-context header
      bridges (`extractTraceContextFromRecord`,
      `injectTraceContextIntoRecord`). The default interpreters
      `runKafkaProducer` and `runKafkaConsumer` are unchanged and
      remain zero-cost for users who do not want tracing. The
      attribute keys and value types match what
      `shibuya-kafka-adapter` already emits, so layering the two
      remains compatible.

Then close the `## Unreleased` heading with a new `## 0.2.0.0 —
2026-MM-DD` line whose date is filled in at release time. The
release-time bump is technically out of scope of this plan but is
trivially the next step.

Acceptance: `cabal sdist` produces an sdist with the updated README
and CHANGELOG. A reader who has only the README can wire up the new
interpreters from the snippet alone.

### Milestone 10 — Outcomes & Retrospective

Fill in the Outcomes & Retrospective section with:

- What was achieved: list the five new modules, the new test-suite,
  the new example, and the version bump.
- What remains: identify any spec attributes this plan did not yet
  emit (likely `messaging.message.id` if the user wants to attach
  one — kafka does not have a native message ID, so this attribute
  is conventionally `<topic>-<partition>-<offset>`; this plan does
  not synthesize that, leaving it to the caller as Shibuya does).
- Lessons learned, if any (most likely candidates: bound-resolution
  surprises in cabal, Span lifecycle subtleties when wrapping
  effectful interpreters).


## Concrete Steps

These are exact commands, with working directory and expected output,
for the implementer to follow. Update this section as work proceeds —
if a command produces output different from what is shown, capture
the divergence in Surprises & Discoveries and update the expected
transcript here.

The working directory for every command in this section is
`/Users/shinzui/Keikaku/bokuno/kafka-effectful` unless stated
otherwise. The dev shell from `nix develop` (or `direnv allow`) must
be active so `cabal`, `ghc-9.12.2`, and `librdkafka` are on `PATH`.

### Verifying the baseline

    cabal build all

Expected: builds cleanly. If it does not, stop and resolve the
existing breakage before proceeding — this plan assumes the master
branch builds.

### Per-milestone build check

After every file edit:

    cabal build

Expected: success. Warnings related to `EffectHandler`'s
`-Wredundant-constraints` are pre-existing (see
`docs/plans/3-harden-interpreters.md`'s Outcomes section); they are
acceptable.

### Test-suite invocation (Milestone 7 onward)

    cabal test --test-show-details=streaming

Expected:

    Test suite kafka-effectful-test: RUNNING...
    Kafka.Effectful.OpenTelemetry
      Semantic
        producerRecordAttributes
          adds messaging.system=kafka:                                OK
          adds messaging.destination.name from topic:                 OK
          adds messaging.operation=send:                              OK
          adds messaging.kafka.destination.partition for SpecifiedPartition: OK
          omits messaging.kafka.destination.partition for UnassignedPartition: OK
          adds messaging.kafka.message.key when valid UTF-8:          OK
          omits messaging.kafka.message.key when invalid UTF-8:       OK
        consumerRecordAttributes
          adds messaging.system=kafka:                                OK
          adds messaging.destination.name from topic:                 OK
          adds messaging.operation=process:                           OK
          adds messaging.kafka.destination.partition:                 OK
          adds messaging.kafka.message.offset:                        OK
          adds messaging.kafka.message.key when valid UTF-8:          OK
      Propagation
        round-trip kafka headers ↔ request headers (case-fold):       OK
        injectTraceContextIntoRecord adds W3C traceparent:            OK
        extractTraceContextFromRecord recovers parent context:        OK
      ShibuyaCompatibility
        consumerRecordAttributes matches Convert.kafkaSpanAttributes
          on messaging.system:                                        OK
          on messaging.kafka.destination.partition (Int64, value):    OK
          on messaging.kafka.message.offset (Int64, value):           OK
    All N tests passed (Ns)

### End-to-end run (Milestone 8)

In one shell, start a Kafka broker (any local docker-compose works).
Then in a second shell:

    cabal run example-otel-tracing -f examples -- \
        --bootstrap-servers localhost:9092 \
        --topic otel-demo

Expected output (trace IDs are 32-hex-char strings, regenerated each
run):

    [otel-tracing] producer trace id: 4bf92f3577b34da6a3ce929d0e0e4736
    [otel-tracing] consumer trace id: 4bf92f3577b34da6a3ce929d0e0e4736
    [otel-tracing] trace IDs match — context propagated through Kafka headers


## Validation and Acceptance

The plan is accepted when all four conditions hold simultaneously:

1.  **Builds cleanly.** `cabal build all` exits zero with no new
    warnings beyond the pre-existing `EffectHandler` redundancy
    warning. `cabal sdist` produces a tarball that re-extracts and
    re-builds successfully.

2.  **Tests pass.** `cabal test` exits zero. The
    `ShibuyaCompatibilityTest` module asserts that
    `consumerRecordAttributes` produces the same three keys with the
    same value types and same values that
    `shibuya-kafka-adapter`'s `kafkaSpanAttributes` produces.

3.  **End-to-end propagation works.** Against a local Kafka broker,
    `cabal run example-otel-tracing -f examples -- ...` produces
    matching producer-side and consumer-side trace IDs. If a Jaeger
    v2 instance is reachable, the trace shows up with the expected
    parent/child shape and the expected attribute set.

4.  **Documented.** A reader who picks up `README.md` cold can wire
    `runKafkaProducerTraced` from the snippet alone, with no
    additional reading required. `CHANGELOG.md` records the change
    under `## Unreleased`. `docs/plans/8-add-opentelemetry-tracing-support.md`
    (this file) has the Progress checklist fully checked, the
    Decision Log updated to reflect any in-flight choices, and the
    Outcomes & Retrospective section filled in.

If only some conditions hold at the end of an implementation
session, capture the gap in Progress (split the relevant item into
"done" and "remaining" entries) and continue in the next session.


## Idempotence and Recovery

All edits are additive: new files, new cabal stanzas, new
build-depends, new exposed-modules. No existing module under
`src/Kafka/Effectful/{Producer,Consumer}/*` is modified at the source
level. If an implementation step goes wrong, the recovery path is to
revert the offending file (`git checkout src/Kafka/Effectful/OpenTelemetry/...`)
and retry.

The cabal file edits are also additive (new entries, no removals);
reverting is a `git checkout kafka-effectful.cabal` away.

The version bump from `0.1.0.0` to `0.2.0.0` happens only in the
cabal file and is reversible via the same path.

The plan does not introduce migrations, schema changes, or
network-side state. The `example-otel-tracing` executable produces a
record on the broker each run, but that is observed by the test
broker and intentional.

If the OTel SDK initialization fails at runtime (for example, an
unreachable OTLP endpoint), the example program prints the failure
and exits non-zero; the production interpreters still emit spans
locally (they hit the no-op no-export tracer provider when none is
configured), so the failure mode is "no spans exported" rather than
"app crashes."


## Interfaces and Dependencies

External libraries used:

- `hs-opentelemetry-api ^>=0.3` — provides `Tracer`, `Span`,
  `SpanArguments`, `SpanKind`, `inSpan''`, `addAttributesToSpanArguments`,
  `Context`, `Context.insertSpan`, `getContext`, `getGlobalTracerProvider`,
  `getTracerProviderPropagators`, `OpenTelemetry.Attributes.{Map, Key}`,
  `OpenTelemetry.Propagator.{extract, inject}`,
  `OpenTelemetry.Context.ThreadLocal.{getContext, attachContext, adjustContext}`.
  Source on disk:
  `/Users/shinzui/Keikaku/hub/haskell/hs-opentelemetry-project/hs-opentelemetry/api`.

- `hs-opentelemetry-semantic-conventions ^>=0.1` — provides the
  `AttributeKey a` constants
  `messaging_system`, `messaging_destination_name`,
  `messaging_operation`, `messaging_kafka_destination_partition`,
  `messaging_kafka_message_offset`, `messaging_kafka_message_key`,
  `messaging_kafka_consumer_group`. Source on disk:
  `/Users/shinzui/Keikaku/hub/haskell/hs-opentelemetry-project/hs-opentelemetry/semantic-conventions`.

- `case-insensitive >=1.2 && <1.3` — provides `Data.CaseInsensitive.{mk,
  foldedCase}` for the `Headers` ↔ `RequestHeaders` bridge.

- `http-types >=0.12 && <0.13` — provides
  `Network.HTTP.Types.RequestHeaders` (a type alias for
  `[(CI ByteString, ByteString)]`).

- `unliftio-core >=0.2 && <0.3` — provides the `MonadUnliftIO`
  superclass `inSpan''` requires; `effectful` already declares the
  `Eff es` instance when `IOE :> es`.

- `hs-opentelemetry-sdk` — test-suite only — provides
  `initializeTracerProvider` so tests can build a real provider with
  the in-memory exporter attached.

- `hs-opentelemetry-exporter-in-memory` — test-suite only — provides
  the in-memory `SpanExporter` that lets tests assert on captured
  spans.

- `hs-opentelemetry-exporter-otlp` — example-only — provides the
  default OTLP HTTP exporter for the end-to-end example program.

- `tasty ^>=1.5`, `tasty-hunit ^>=0.10` — test driver.

Function and module signatures that must exist at the end of each
milestone:

After Milestone 2, in `src/Kafka/Effectful/OpenTelemetry/Semantic.hs`:

    kafkaMessagingSystem        :: Text
    producerOperationName       :: Text
    consumerOperationName       :: Text
    producerSpanName            :: TopicName -> Text
    consumerSpanName            :: TopicName -> Text
    producerRecordAttributes    :: ProducerRecord -> AttributeMap
    consumerRecordAttributes    :: ConsumerRecord (Maybe ByteString) (Maybe ByteString) -> AttributeMap

After Milestone 3, in `src/Kafka/Effectful/OpenTelemetry/Propagation.hs`:

    kafkaHeadersToRequestHeaders   :: Headers -> RequestHeaders
    requestHeadersToKafkaHeaders   :: RequestHeaders -> Headers
    extractTraceContextFromRecord  :: ConsumerRecord k v -> Context -> IO Context
    injectTraceContextIntoRecord   :: Context -> ProducerRecord -> IO ProducerRecord

After Milestone 4, in
`src/Kafka/Effectful/OpenTelemetry/Producer/Interpreter.hs`:

    runKafkaProducerTraced ::
      (IOE :> es, Error KafkaError :> es) =>
      Tracer ->
      ProducerProperties ->
      Eff (KafkaProducer : es) a ->
      Eff es a

After Milestone 5, in
`src/Kafka/Effectful/OpenTelemetry/Consumer/Interpreter.hs`:

    runKafkaConsumerTraced ::
      (IOE :> es, Error KafkaError :> es) =>
      Tracer ->
      ConsumerProperties ->
      Subscription ->
      Eff (KafkaConsumer : es) a ->
      Eff es a

After Milestone 6, the facade `Kafka.Effectful.OpenTelemetry` re-exports
the union of those signatures.

The existing `KafkaProducer` and `KafkaConsumer` GADTs from
`Kafka.Effectful.{Producer,Consumer}.Effect` are not modified. The
new interpreters are alternatives to the existing
`runKafkaProducer` / `runKafkaConsumer`; both pairs continue to
co-exist in the library.
