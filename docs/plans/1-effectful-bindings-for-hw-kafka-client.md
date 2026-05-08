---
id: 1
slug: effectful-bindings-for-hw-kafka-client
title: "Effectful bindings for hw-kafka-client"
kind: exec-plan
created_at: 2026-04-01T14:35:50Z
intention: "intention_01km3c2s7xeamb7gkfjkve90ma"
---


# Effectful bindings for hw-kafka-client

This ExecPlan is a living document. The sections Progress, Surprises & Discoveries,
Decision Log, and Outcomes & Retrospective must be kept up to date as work proceeds.

This document is maintained in accordance with `.claude/skills/exec-plan/PLANS.md`.


## Purpose / Big Picture

After this work is complete, Haskell applications built on the effectful ecosystem will be
able to produce and consume Apache Kafka messages through typed, composable effects rather
than raw IO. A developer adds a `KafkaProducer :> es` or `KafkaConsumer :> es` constraint,
calls functions like `produceMessage` or `pollMessage`, and the effectful handler manages
the underlying hw-kafka-client resources (connection, configuration, cleanup). The library
re-exports all necessary hw-kafka-client types so users do not need to import hw-kafka-client
directly for typical use.

Verification is straightforward: the package compiles against GHC 9.12 with GHC2024, the
cabal file is well-formed, and a small example program type-checks demonstrating both
producer and consumer effects wired into an `Eff` computation.


## Progress

- [x] Milestone 1: Project scaffold (cabal file, directory structure, flake.nix updates) — 2026-04-01
- [x] Milestone 2: Producer effect and interpreter — 2026-04-01
- [x] Milestone 3: Consumer effect and interpreter — 2026-04-01
- [x] Milestone 4: Facade re-export module and validation — 2026-04-01


## Surprises & Discoveries

- `produceMessageBatch` is not exported from `Kafka.Producer` in hw-kafka-client 5.3.0 on
  Hackage, despite being present in the user's local source checkout. The function was
  likely added after the 5.3.0 release. Removed `ProduceMessageBatch` from the effect GADT.

- `Kafka.Consumer.Callbacks` and `Kafka.Producer.Callbacks` are hidden modules in
  hw-kafka-client. They are re-exported through `Kafka.Consumer.ConsumerProperties` and
  `Kafka.Producer.ProducerProperties` respectively. The facade modules import callbacks
  through the properties modules.

- `pausePartitions` and `resumePartitions` return a bare `KafkaError` (always wrapping
  `KafkaResponseError`), not `Maybe KafkaError`. The interpreter checks for `RdKafkaRespErrNoError`
  (enum value 0) to distinguish success from failure.

- The `EffectHandler` type alias in effectful-core 2.6 triggers a `-Wredundant-constraints`
  warning about `e :> localEs` when the handler's own constraints are declared. This is a
  known quirk and does not affect correctness.


## Decision Log

- Decision: Use Dynamic dispatch for both KafkaProducer and KafkaConsumer effects.
  Rationale: This matches the user's established pattern in pgmq-effectful and kiroku-store,
  where all effects use Dynamic dispatch with GADT constructors. Dynamic dispatch provides
  flexibility for future alternative interpreters (e.g., test mocks, traced variants).
  Date: 2026-04-01

- Decision: Separate KafkaProducer and KafkaConsumer into distinct effects rather than a
  single unified Kafka effect.
  Rationale: Producers and consumers have fundamentally different lifecycles and resource
  requirements in hw-kafka-client. A producer requires `ProducerProperties` and yields a
  `KafkaProducer` handle; a consumer requires `ConsumerProperties` plus a `Subscription`
  and yields a `KafkaConsumer` handle. Separating them allows applications to constrain only
  the capability they need (e.g., a service that only produces does not carry a consumer in
  its effect stack). This also mirrors hw-kafka-client's own module split
  (`Kafka.Producer` / `Kafka.Consumer`).
  Date: 2026-04-01

- Decision: Defer transaction, metadata, topic management, and dump APIs to future work.
  Rationale: The core value of this package is type-safe produce/consume in effectful. The
  transaction API (`Kafka.Transaction`) crosses both producer and consumer handles and adds
  design complexity. Metadata queries (`Kafka.Metadata`) use a `HasKafka` type class that
  abstracts over both handle types. These are better addressed in a follow-up plan once the
  core effects are stable and validated.
  Date: 2026-04-01

- Decision: Handle hw-kafka-client's `Either KafkaError a` and `Maybe KafkaError` return
  patterns by throwing errors through effectful's `Error KafkaError` effect rather than
  exposing raw `Either`/`Maybe` in the effect interface.
  Rationale: This is consistent with how pgmq-effectful handles errors (the interpreter
  requires `Error PgmqError :> es` and calls `throwError` on failure). It also gives
  callers the choice: they can `runError` to catch errors as `Either`, or let them
  propagate. hw-kafka-client's `KafkaError` type is already well-structured with multiple
  constructors that carry context.
  Date: 2026-04-01

- Decision: The interpreter for KafkaConsumer will manage the consumer handle lifecycle
  using a Static effect with `WithSideEffects`, holding the `KafkaConsumer` handle in
  `StaticRep`. The Dynamic effect operations then delegate to the handle stored in
  the static layer. The same approach applies to KafkaProducer.
  Rationale: The hw-kafka-client library requires creating a handle (`newConsumer`/
  `newProducer`) and closing it (`closeConsumer`/`closeProducer`). The interpreter needs
  to hold this handle for the duration of the effect scope. Using `reinterpret` with a
  helper static effect to store the handle is the idiomatic effectful pattern for
  resource-holding interpreters (similar to how `beam-sqlite-effectful` holds a connection
  pool in `StaticRep`). However, since hw-kafka-client handles are not pools and cannot
  be shared across threads safely without care, the interpreter acquires the handle on
  entry and releases it when the effect scope ends via bracket semantics built into
  `evalStaticRep`.
  Date: 2026-04-01

- Decision: Target GHC 9.12+ with GHC2024 default language and match pgmq-effectful's
  extension and warning configuration.
  Rationale: The user explicitly stated GHC 9.12+ with GHC2024. The pgmq-effectful cabal
  file provides a proven, consistent baseline for extensions and warnings.
  Date: 2026-04-01

- Decision: Drop `ProduceMessageBatch` from the effect — `produceMessageBatch` is not
  exported in hw-kafka-client 5.3.0 on Hackage.
  Rationale: The function exists in the user's local source but was not included in the
  5.3.0 Hackage release. Building against Hackage is the correct default.
  Date: 2026-04-01

- Decision: Use `bracket` + `interpret` rather than `reinterpret` with a helper static
  effect for resource management.
  Rationale: Simpler to implement. `bracket` acquires the handle before entering `interpret`
  and ensures cleanup. The handle is captured in the `handleProducer`/`handleConsumer`
  closures. No need for `StaticRep` machinery since the handle is not accessed outside
  the handler.
  Date: 2026-04-01


## Outcomes & Retrospective

All four milestones completed. The package compiles cleanly under GHC 9.12.2 with GHC2024
and all warnings enabled. The only warnings are from the `EffectHandler` type alias in
effectful-core triggering `-Wredundant-constraints` — these are upstream and not actionable.

The package provides 7 modules:
- `Kafka.Effectful` — combined facade
- `Kafka.Effectful.Producer` / `Kafka.Effectful.Consumer` — scoped facades
- `Kafka.Effectful.Producer.Effect` / `Kafka.Effectful.Consumer.Effect` — effect GADTs
- `Kafka.Effectful.Producer.Interpreter` / `Kafka.Effectful.Consumer.Interpreter` — handlers

Future work: add `ProduceMessageBatch` when hw-kafka-client exports it, transaction API,
metadata queries, topic management, and OpenTelemetry-traced interpreter variants.


## Context and Orientation

This section describes every piece of context needed to implement the plan.

The project lives at the repository root. It was scaffolded with Seihou's `nix-haskell-flake`
module and currently contains only Nix configuration (flake.nix, treefmt.nix) and Claude
skill directories. There are no Haskell source files and no cabal file yet.

The Nix flake at `flake.nix` sets up a dev shell with GHC 9.12 (`ghc912`), cabal-install,
zlib, pkg-config, and HLS. It references `haskellPackages.kafka-effectful` as the default
package, which does not yet exist in the Nix package set. The flake needs to be updated to
add `rdkafka` (the C library that hw-kafka-client binds to) as a native dependency in the
dev shell, and to overlay the Haskell package set so that `hw-kafka-client` and
`kafka-effectful` can be built.

hw-kafka-client is a Haskell binding to Apache Kafka via librdkafka. Its source is available
at `/Users/shinzui/Keikaku/hub/haskell/hw-kafka-client-project`. The library exposes its
public API through these key modules:

`Kafka.Producer` provides `newProducer` (creates a `KafkaProducer` handle from
`ProducerProperties`), `closeProducer` (flushes and closes), `produceMessage` (sends a
single `ProducerRecord` asynchronously, returns `Maybe KafkaError` for pre-flight errors),
`produceMessage'` (sends with a delivery callback), and `produceMessageBatch` (sends
multiple records, returns failed ones). `flushProducer` explicitly drains the outbound
queue.

`Kafka.Consumer` provides `newConsumer` (creates a `KafkaConsumer` handle from
`ConsumerProperties` and `Subscription`, returns `Either KafkaError KafkaConsumer`),
`closeConsumer` (returns `Maybe KafkaError`), `pollMessage` (polls one message with a
timeout), `pollMessageBatch` (polls a batch), `commitOffsetMessage` / `commitAllOffsets` /
`commitPartitionsOffsets` (commit offsets either synchronously or asynchronously),
`storeOffsets` / `storeOffsetMessage` (store offsets locally without committing), `assign`
(manual partition assignment), `subscription` and `assignment` (query current state),
`pausePartitions` / `resumePartitions`, `seekPartitions`, `committed`, `position`, and
`rewindConsumer`.

All hw-kafka-client operations are polymorphic over `MonadIO m` and return either
`Either KafkaError a` (operation may fail) or `Maybe KafkaError` (Nothing = success,
Just = error). The library's types live in `Kafka.Types`, `Kafka.Consumer.Types`,
`Kafka.Producer.Types`, `Kafka.Consumer.ConsumerProperties`, `Kafka.Consumer.Subscription`,
and `Kafka.Producer.ProducerProperties`.

The effectful library (source at `/Users/shinzui/Keikaku/hub/haskell/effectful-project`) is
a typed effect system for Haskell. Effects are defined as GADTs with a `DispatchOf` type
family instance set to either `'Static` or `'Dynamic`. Dynamic effects use `send` to
dispatch operations and `interpret` or `reinterpret` to handle them. The `Eff` monad
carries a type-level list of effects (`es`), and the `(:>)` constraint asserts membership.
`IOE` is the effect that permits IO access. `unsafeEff_` lifts raw `IO a` into `Eff es a`
for use within effect handlers.

The user's existing effectful library, pgmq-effectful (at
`/Users/shinzui/Keikaku/bokuno/libraries/pgmq-hs-project/pgmq-hs/pgmq-effectful/`), is the
primary reference for conventions. It uses:

- Dynamic dispatch with a single GADT (`data Pgmq :: Effect where ...`)
- Each GADT constructor maps one-to-one to a public API function
- Thin wrapper functions (`createQueue = send . CreateQueue`)
- An interpreter that takes a resource handle (connection pool) and calls `interpret`
  with exhaustive pattern matching, delegating to the underlying library
- Errors handled via `Error PgmqError :> es` constraint with `throwError`
- A facade module (`Pgmq.Effectful`) that re-exports everything

The cabal file uses `cabal-version: 3.4`, `default-language: GHC2024`, and these
default-extensions: `DataKinds`, `DuplicateRecordFields`, `GADTs`,
`GeneralisedNewtypeDeriving`, `ImportQualifiedPost`, `LambdaCase`, `NoFieldSelectors`,
`OverloadedRecordDot`, `OverloadedStrings`, `TypeFamilies`, `TypeOperators`. The warnings
stanza includes `-Wall -Wcompat -Widentities -Wincomplete-uni-patterns
-Wincomplete-record-updates -Wredundant-constraints -fhide-source-paths
-Wmissing-export-lists -Wpartial-fields -Wmissing-deriving-strategies`.


## Plan of Work

The work is organized into four milestones. Each milestone is independently verifiable.


### Milestone 1: Project Scaffold

This milestone creates the cabal file, directory structure, and updates the Nix flake so
that an empty library compiles. At the end, `cabal build` succeeds in the nix dev shell
with an empty module.

Create the directory `src/Kafka/Effectful/` for source files. Create the cabal file
`kafka-effectful.cabal` at the repository root. The cabal file should follow pgmq-effectful's
structure exactly: `cabal-version: 3.4`, a `common warnings` stanza with the same GHC
options, a `library` stanza importing warnings, `default-language: GHC2024`, and the same
`default-extensions` list. The initial `exposed-modules` list will contain a single
placeholder module `Kafka.Effectful`.

The build dependencies for the library stanza will be:

    base >= 4.21 && < 5
    bytestring >= 0.11 && < 0.13
    effectful-core ^>= 2.5 || ^>= 2.6
    hw-kafka-client >= 5.3 && < 6
    text >= 2.0 && < 2.2

The lower bound on `base` is 4.21 because that is what GHC 9.12 ships. The
`hw-kafka-client` bound covers version 5.3.x which is the current version. The
`effectful-core` bounds match pgmq-effectful's.

Create `src/Kafka/Effectful.hs` as a minimal module with an empty export list:

    module Kafka.Effectful () where

Update `flake.nix` to add `pkgs.rdkafka` to `nativeBuildInputs` in the dev shell, since
hw-kafka-client requires the librdkafka C library at link time.

Verification: enter the dev shell (`nix develop`), run `cabal build`, and observe successful
compilation with no errors.


### Milestone 2: Producer Effect and Interpreter

This milestone defines the `KafkaProducer` effect and its interpreter. At the end, a program
can produce messages to Kafka through the effectful effect system.

Create `src/Kafka/Effectful/Producer/Effect.hs` containing the `KafkaProducer` effect GADT.
The constructors correspond to the core producer operations from `Kafka.Producer`:

    data KafkaProducer :: Effect where
      ProduceMessage :: ProducerRecord -> KafkaProducer m ()
      ProduceMessageBatch :: [ProducerRecord] -> KafkaProducer m [(ProducerRecord, KafkaError)]
      FlushProducer :: KafkaProducer m ()

    type instance DispatchOf KafkaProducer = 'Dynamic

The `ProduceMessage` constructor returns `()` rather than `Maybe KafkaError` because errors
are thrown via the `Error KafkaError` effect. `ProduceMessageBatch` returns the list of
failed records (matching hw-kafka-client's `produceMessageBatch` semantics where failures
are per-record rather than wholesale).

Alongside the GADT, define the thin wrapper functions:

    produceMessage :: (KafkaProducer :> es) => ProducerRecord -> Eff es ()
    produceMessage = send . ProduceMessage

    produceMessageBatch :: (KafkaProducer :> es) => [ProducerRecord] -> Eff es [(ProducerRecord, KafkaError)]
    produceMessageBatch = send . ProduceMessageBatch

    flushProducer :: (KafkaProducer :> es) => Eff es ()
    flushProducer = send FlushProducer

Create `src/Kafka/Effectful/Producer/Interpreter.hs` containing the interpreter. The
interpreter takes `ProducerProperties`, acquires a `Kafka.Producer.KafkaProducer` handle
using `newProducer`, and interprets the effect. On scope exit it calls `closeProducer`.

The implementation uses effectful's `bracket` (from `Effectful.Exception`) to ensure cleanup:

    runKafkaProducer ::
      (IOE :> es, Error KafkaError :> es) =>
      ProducerProperties ->
      Eff (KafkaProducer : es) a ->
      Eff es a
    runKafkaProducer props action = do
      result <- liftIO $ Kafka.Producer.newProducer props
      case result of
        Left err -> throwError err
        Right producer ->
          Effectful.Exception.bracket
            (pure producer)
            (\p -> liftIO $ Kafka.Producer.closeProducer p)
            (\p -> interpret (handleProducer p) action)

The handler function pattern-matches each constructor:

    handleProducer ::
      (IOE :> es, Error KafkaError :> es) =>
      Kafka.Producer.KafkaProducer ->
      EffectHandler KafkaProducer es
    handleProducer producer _ = \case
      ProduceMessage record -> do
        result <- liftIO $ Kafka.Producer.produceMessage producer record
        case result of
          Nothing -> pure ()
          Just err -> throwError err
      ProduceMessageBatch records ->
        liftIO $ Kafka.Producer.produceMessageBatch producer records
      FlushProducer ->
        liftIO $ Kafka.Producer.flushProducer producer

Verification: add both modules to the cabal file's `exposed-modules`, run `cabal build`,
and confirm it compiles.


### Milestone 3: Consumer Effect and Interpreter

This milestone defines the `KafkaConsumer` effect and its interpreter. At the end, a
program can consume messages from Kafka and manage offsets through the effectful effect
system.

Create `src/Kafka/Effectful/Consumer/Effect.hs` containing the `KafkaConsumer` effect GADT.
The constructors cover the core consumer operations from `Kafka.Consumer`:

    data KafkaConsumer :: Effect where
      PollMessage
        :: Timeout
        -> KafkaConsumer m (ConsumerRecord (Maybe ByteString) (Maybe ByteString))
      PollMessageBatch
        :: Timeout
        -> BatchSize
        -> KafkaConsumer m [Either KafkaError (ConsumerRecord (Maybe ByteString) (Maybe ByteString))]
      CommitOffsetMessage
        :: OffsetCommit
        -> ConsumerRecord k v
        -> KafkaConsumer m ()
      CommitAllOffsets
        :: OffsetCommit
        -> KafkaConsumer m ()
      CommitPartitionsOffsets
        :: OffsetCommit
        -> [TopicPartition]
        -> KafkaConsumer m ()
      StoreOffsets
        :: [TopicPartition]
        -> KafkaConsumer m ()
      StoreOffsetMessage
        :: ConsumerRecord k v
        -> KafkaConsumer m ()
      Assign
        :: [TopicPartition]
        -> KafkaConsumer m ()
      PausePartitions
        :: [(TopicName, PartitionId)]
        -> KafkaConsumer m ()
      ResumePartitions
        :: [(TopicName, PartitionId)]
        -> KafkaConsumer m ()
      SeekPartitions
        :: [TopicPartition]
        -> Timeout
        -> KafkaConsumer m ()
      Committed
        :: Timeout
        -> [(TopicName, PartitionId)]
        -> KafkaConsumer m [TopicPartition]
      Position
        :: [(TopicName, PartitionId)]
        -> KafkaConsumer m [TopicPartition]
      Assignment
        :: KafkaConsumer m (Map TopicName [PartitionId])
      Subscription
        :: KafkaConsumer m [(TopicName, SubscribedPartitions)]

    type instance DispatchOf KafkaConsumer = 'Dynamic

For the `PollMessage` constructor, the return type is the unwrapped success case. Errors
(including `KafkaResponseError RdKafkaRespErrTimedOut` for poll timeouts) are thrown via
`Error KafkaError`. For `PollMessageBatch`, the per-message errors remain in the `Either`
since individual message failures are expected in a batch and should not abort the entire
poll.

For operations that return `Maybe KafkaError` in hw-kafka-client (like `commitAllOffsets`,
`closeConsumer`, `storeOffsets`, etc.), the effect returns `()` on success and throws via
`Error KafkaError` on `Just err`.

For `pausePartitions` and `resumePartitions`, hw-kafka-client returns a bare `KafkaError`
(not `Maybe`); the interpreter throws that error.

Thin wrapper functions follow the same pattern as the producer.

Create `src/Kafka/Effectful/Consumer/Interpreter.hs` with the interpreter:

    runKafkaConsumer ::
      (IOE :> es, Error KafkaError :> es) =>
      ConsumerProperties ->
      Subscription ->
      Eff (KafkaConsumer : es) a ->
      Eff es a

The interpreter acquires a consumer handle via `newConsumer`, brackets with `closeConsumer`,
and interprets the effect by delegating each constructor to the corresponding
hw-kafka-client function. The handler translates `Either KafkaError a` results into
`throwError` / `pure` and `Maybe KafkaError` results into `throwError` on `Just`.

Verification: add both modules to the cabal file, run `cabal build`, confirm compilation.


### Milestone 4: Facade Re-export Module and Validation

This milestone creates the facade module that re-exports everything a user needs from a
single import, and validates the complete package.

Update `src/Kafka/Effectful.hs` to re-export:

- `KafkaProducer` effect type (not constructors)
- `runKafkaProducer` interpreter
- All producer operation functions
- `KafkaConsumer` effect type (not constructors)
- `runKafkaConsumer` interpreter
- All consumer operation functions
- All relevant hw-kafka-client types needed to use the effects: `ProducerRecord`,
  `ProducerProperties`, `ConsumerProperties`, `Subscription`, `ConsumerRecord`,
  `KafkaError`, `TopicName`, `BrokerAddress`, `Timeout`, `BatchSize`, `Offset`,
  `OffsetCommit`, `TopicPartition`, `PartitionId`, `SubscribedPartitions`,
  `ProducePartition`, `Headers`, `ConsumerGroupId`, `OffsetReset`, configuration
  builder functions (`brokersList`, `groupId`, `logLevel`, etc.), and subscription
  builders (`topics`, `offsetReset`).

This follows the pgmq-effectful pattern where `Pgmq.Effectful` is a one-stop import.

Create a separate `Kafka.Effectful.Producer` facade that re-exports only producer-related
items, and `Kafka.Effectful.Consumer` for consumer-related items. This gives users the
choice of importing everything via `Kafka.Effectful` or scoping to one side.

Verification: run `cabal build` and `cabal check` (if applicable). Inspect the build output
for any warnings. Verify that the module dependency graph is clean (no circular imports,
no orphan instances).

As a final validation, create a small example module (not exposed, placed in an `example/`
directory or in the plan itself) that demonstrates wiring both effects:

    example :: IO ()
    example =
      runEff
        . runError @KafkaError
        . runKafkaConsumer consumerProps consumerSub
        . runKafkaProducer producerProps
        $ do
          msg <- pollMessage (Timeout 1000)
          produceMessage ProducerRecord
            { prTopic = TopicName "output"
            , prPartition = UnassignedPartition
            , prKey = crKey msg
            , prValue = crValue msg
            , prHeaders = mempty
            }
          commitOffsetMessage OffsetCommit msg

This example should type-check, demonstrating that both effects compose in a single `Eff`
computation.


## Concrete Steps

All commands are run from the repository root:
`/Users/shinzui/Keikaku/bokuno/libraries/haskell/kafka-effectful`.

Milestone 1:

    mkdir -p src/Kafka/Effectful/Producer
    mkdir -p src/Kafka/Effectful/Consumer

Create `kafka-effectful.cabal` with the content specified in Milestone 1.
Create `src/Kafka/Effectful.hs` with the empty module.

Update `flake.nix` to add `pkgs.rdkafka` to `nativeBuildInputs`.

    nix develop
    cabal build

Expected: compilation succeeds with a trivial library containing one empty module.

    $ cabal build
    Build profile: ...
    Resolving dependencies...
    Building library for kafka-effectful-0.1.0.0...
    [1 of 1] Compiling Kafka.Effectful

Milestone 2:

Create `src/Kafka/Effectful/Producer/Effect.hs` and
`src/Kafka/Effectful/Producer/Interpreter.hs` with the content specified above.
Add both modules to `exposed-modules` in the cabal file.

    cabal build

Expected: compilation succeeds with three modules.

Milestone 3:

Create `src/Kafka/Effectful/Consumer/Effect.hs` and
`src/Kafka/Effectful/Consumer/Interpreter.hs` with the content specified above.
Add both modules to `exposed-modules` in the cabal file.

    cabal build

Expected: compilation succeeds with five modules.

Milestone 4:

Update `src/Kafka/Effectful.hs` to the full facade re-export.
Create `src/Kafka/Effectful/Producer.hs` (producer facade) and
`src/Kafka/Effectful/Consumer.hs` (consumer facade).
Add new modules to `exposed-modules`.

    cabal build

Expected: compilation succeeds with all eight modules. No warnings about missing exports
or unused imports.


## Validation and Acceptance

The primary acceptance criterion is that the package compiles cleanly under GHC 9.12 with
GHC2024 and all warnings enabled. Since this package wraps a library that requires a live
Kafka broker for runtime testing, compile-time validation is the primary gate for this plan.

Run `cabal build` after each milestone. Every milestone must compile with zero errors and
ideally zero warnings. If warnings appear from hw-kafka-client itself (deprecation notices,
etc.), they are acceptable as long as they do not originate from kafka-effectful code.

After Milestone 4, verify the complete module graph by checking that:

1. `import Kafka.Effectful` brings all public API items into scope.
2. `import Kafka.Effectful.Producer` brings only producer items.
3. `import Kafka.Effectful.Consumer` brings only consumer items.
4. No module imports are unused.

A type-checking example program (shown in Milestone 4) confirms that the effects compose
correctly and that all necessary types are re-exported.


## Idempotence and Recovery

Every step in this plan is file creation or file editing. Running any step twice produces
the same result. `cabal build` is always safe to re-run. If compilation fails at any point,
the fix is to correct the source file and rebuild. There are no destructive operations, no
migrations, and no external state to manage.

If the Nix flake update causes issues entering the dev shell, the fix is to ensure
`pkgs.rdkafka` is spelled correctly (it is the Nix package name for librdkafka) and that
the Nix flake lock is up to date (`nix flake update`).


## Interfaces and Dependencies

The package depends on two external Haskell libraries:

hw-kafka-client (>= 5.3, < 6): Provides the underlying Kafka client. Key modules are
`Kafka.Producer` and `Kafka.Consumer`. Key types are `KafkaProducer` (opaque handle),
`KafkaConsumer` (opaque handle), `ProducerProperties`, `ConsumerProperties`, `Subscription`,
`ProducerRecord`, `ConsumerRecord`, `KafkaError`, `TopicName`, `BrokerAddress`, `Timeout`,
`BatchSize`, `Offset`, `OffsetCommit`, `TopicPartition`, `PartitionId`,
`SubscribedPartitions`, `ProducePartition`, `Headers`, `ConsumerGroupId`, and `OffsetReset`.
Functions: `newProducer`, `closeProducer`, `produceMessage`, `produceMessageBatch`,
`flushProducer`, `newConsumer`, `closeConsumer`, `pollMessage`, `pollMessageBatch`,
`commitOffsetMessage`, `commitAllOffsets`, `commitPartitionsOffsets`, `storeOffsets`,
`storeOffsetMessage`, `assign`, `pausePartitions`, `resumePartitions`, `seekPartitions`,
`committed`, `position`, `assignment`, `subscription`.

effectful-core (^>= 2.5 || ^>= 2.6): Provides the effect system. Key types are `Eff`,
`Effect`, `(:>)`, `IOE`, `DispatchOf`, `EffectHandler`. Key functions are `send`,
`interpret`, `liftIO`, `throwError`, `runError`, `runEff`. From `Effectful.Exception`:
`bracket`.

The package also transitively depends on `bytestring` (for `ByteString` in `ConsumerRecord`
and `ProducerRecord` fields), `text` (for `TopicName` and other newtypes), and
`containers` (for `Map` in `assignment` return type).

At the end of Milestone 4, these modules and their key exports must exist:

In `src/Kafka/Effectful/Producer/Effect.hs`:

    data KafkaProducer :: Effect
    produceMessage :: (KafkaProducer :> es) => ProducerRecord -> Eff es ()
    produceMessageBatch :: (KafkaProducer :> es) => [ProducerRecord] -> Eff es [(ProducerRecord, KafkaError)]
    flushProducer :: (KafkaProducer :> es) => Eff es ()

In `src/Kafka/Effectful/Producer/Interpreter.hs`:

    runKafkaProducer ::
      (IOE :> es, Error KafkaError :> es) =>
      ProducerProperties ->
      Eff (KafkaProducer : es) a ->
      Eff es a

In `src/Kafka/Effectful/Consumer/Effect.hs`:

    data KafkaConsumer :: Effect
    pollMessage :: (KafkaConsumer :> es) => Timeout -> Eff es (ConsumerRecord (Maybe ByteString) (Maybe ByteString))
    pollMessageBatch :: (KafkaConsumer :> es) => Timeout -> BatchSize -> Eff es [Either KafkaError (ConsumerRecord (Maybe ByteString) (Maybe ByteString))]
    commitOffsetMessage :: (KafkaConsumer :> es) => OffsetCommit -> ConsumerRecord k v -> Eff es ()
    commitAllOffsets :: (KafkaConsumer :> es) => OffsetCommit -> Eff es ()
    commitPartitionsOffsets :: (KafkaConsumer :> es) => OffsetCommit -> [TopicPartition] -> Eff es ()
    storeOffsets :: (KafkaConsumer :> es) => [TopicPartition] -> Eff es ()
    storeOffsetMessage :: (KafkaConsumer :> es) => ConsumerRecord k v -> Eff es ()
    assign :: (KafkaConsumer :> es) => [TopicPartition] -> Eff es ()
    pausePartitions :: (KafkaConsumer :> es) => [(TopicName, PartitionId)] -> Eff es ()
    resumePartitions :: (KafkaConsumer :> es) => [(TopicName, PartitionId)] -> Eff es ()
    seekPartitions :: (KafkaConsumer :> es) => [TopicPartition] -> Timeout -> Eff es ()
    committed :: (KafkaConsumer :> es) => Timeout -> [(TopicName, PartitionId)] -> Eff es [TopicPartition]
    position :: (KafkaConsumer :> es) => [(TopicName, PartitionId)] -> Eff es [TopicPartition]
    assignment :: (KafkaConsumer :> es) => Eff es (Map TopicName [PartitionId])
    subscription :: (KafkaConsumer :> es) => Eff es [(TopicName, SubscribedPartitions)]

In `src/Kafka/Effectful/Consumer/Interpreter.hs`:

    runKafkaConsumer ::
      (IOE :> es, Error KafkaError :> es) =>
      ConsumerProperties ->
      Subscription ->
      Eff (KafkaConsumer : es) a ->
      Eff es a

In `src/Kafka/Effectful.hs`:

    Re-exports all of the above plus all hw-kafka-client types needed for typical use.

In `src/Kafka/Effectful/Producer.hs`:

    Re-exports producer effect, interpreter, and relevant hw-kafka-client producer types.

In `src/Kafka/Effectful/Consumer.hs`:

    Re-exports consumer effect, interpreter, and relevant hw-kafka-client consumer types.
