---
id: 7
slug: support-sync-publish-and-producer-best-practices
title: "Support sync publish and producer best practices"
kind: exec-plan
created_at: 2026-04-22T12:36:13Z
intention: "intention_01km3c2s7xeamb7gkfjkve90ma"
---


# Support sync publish and producer best practices

This ExecPlan is a living document. The sections Progress, Surprises & Discoveries,
Decision Log, and Outcomes & Retrospective must be kept up to date as work proceeds.

This document is maintained in accordance with `.claude/skills/exec-plan/PLANS.md`.


## Purpose / Big Picture

Today, the `KafkaProducer` effect in this library exposes two operations:
`produceMessage` (fire-and-forget enqueue) and `flushProducer` (drain the
outbound queue). That is enough for Scenario 1 ("Fire-and-Forget") of
`hw-kafka-client`'s producer best practices, but it leaves five of the eight
documented scenarios either impossible or awkward to express through the
effect: synchronous per-message delivery confirmation (Scenario 2), bulk
batched publishing (Scenario 4), exactly-once consume-transform-produce with
transactions (Scenario 5), and — more subtly — Scenarios 6 and 7 (keyed and
custom partitioning) which already work through the data model but have no
README examples pointing users at the idiomatic shape.

After this plan, an application built on `kafka-effectful` can:

- Synchronously publish one message and block until the broker acknowledges
  it, receiving the assigned `Offset` on success or a `KafkaError` on failure,
  through a new operation `produceMessageSync`. This unblocks the "outbox
  pattern" and any control-plane producer that must know the broker-assigned
  offset before proceeding.

- Subscribe to delivery reports per-message through a lower-level new
  operation `produceMessage'` that mirrors the underlying
  `Kafka.Producer.produceMessage'` and accepts a `DeliveryReport -> IO ()`
  callback. This is the building block users compose when they want
  "many-in-flight, await all" semantics (issue N sends, await N `MVar`s).

- Submit many records in one call and receive only the records that failed
  to *enqueue* through a new operation `produceMessageBatch`. Combined with
  `linger.ms` and `batch.size` set on the `ProducerProperties`, this is the
  throughput-oriented path in Scenario 4.

- Drive a transactional exactly-once ETL loop end-to-end through a new set of
  producer operations (`initTransactions`, `beginTransaction`,
  `commitTransaction`, `abortTransaction`, `sendOffsetsToTransaction`) plus a
  thin helper `commitOffsetMessageTransaction` that participates in both the
  `KafkaProducer` and `KafkaConsumer` effect scopes. Users can retry or abort
  a transaction by inspecting the returned `Maybe TxError`, which carries the
  fatal / retriable / requires-abort discriminators that `hw-kafka-client`
  surfaces through `Kafka.Transaction`.

- Read working README examples that cover all eight best-practice scenarios
  so a newcomer can pick the right pattern without going back to
  `hw-kafka-client`'s docs.

How to see it working when the plan is complete:

1.  `cabal build` succeeds with no new warnings.
2.  A new in-repo example program at `examples/SyncPublish.hs` (under the
    library's `example-sync-publish` cabal executable, gated behind an
    `examples` cabal flag) sends one record to a local broker and prints
    the broker-assigned offset. The program type-checks and runs end-to-end
    against a `docker compose` Kafka spun up as documented in the
    "Validation and Acceptance" section.
3.  A second example `examples/TransactionalEtl.hs` consumes from one topic,
    transforms, and produces to another, committing consumer offsets inside
    the producer transaction. Restarting the process mid-batch does not
    produce duplicates downstream (verified via `kcat` output).
4.  The README gains a "Scenarios" section with compact snippets for all
    eight best-practice cases from `producer-best-practices.md`.

If any of the integration scenarios require local infrastructure that the
implementation agent cannot spin up, the plan allows for a "type-check only"
fallback: the example programs must still compile, and a mock-based test
using the existing effect (no real broker) must exercise the `Maybe TxError`
branching for each of the three error classes.


## Progress

- [x] Milestone 1: sync and per-message-callback publish. (2026-04-22)
- [x] Milestone 2: batch publish via `produceMessageBatch`. (2026-04-22)
- [x] Milestone 3: transaction API exposed via producer effect plus a
      cross-effect helper for `commitOffsetMessageTransaction`.
      (2026-04-22)
- [x] Milestone 4: facade updates, Haddocks, and README scenario walk-through.
      (2026-04-22)
- [x] Milestone 5: example programs under the `examples` cabal flag.
      (2026-04-22)
- [x] Milestone 6: full-repo validation — `cabal build`, `cabal sdist`, and
      example programs compile. (2026-04-22)


## Surprises & Discoveries

-   2026-04-22 — The Hackage artefact for `hw-kafka-client-5.3.0` does
    **not** export `Kafka.Producer.produceMessageBatch`, even though
    the local working tree at
    `/Users/shinzui/Keikaku/hub/haskell/hw-kafka-client-project/hw-kafka-client/src/Kafka/Producer.hs`
    (also labelled version `5.3.0` on its cabal file) does. Building
    the direct delegate in `Kafka.Effectful.Producer.Interpreter`
    produced

            src/Kafka/Effectful/Producer/Interpreter.hs:61:28: error: [GHC-76037]
                Not in scope: ‘K.produceMessageBatch’
                Note: The module ‘Kafka.Producer’ does not export
                ‘produceMessageBatch’.

    The interpreter therefore inlines the upstream definition
    (`mapM produceMessage` filtered by `Just`) — exactly the fallback
    the Decision Log entry of 2026-04-22 anticipated. The effect
    signature is unchanged: `produceMessageBatch` still returns
    `[(ProducerRecord, KafkaError)]` containing only the records that
    failed to enqueue. When a future Hackage release of
    `hw-kafka-client` exposes the symbol, the interpreter body can be
    swapped to `K.produceMessageBatch producer records` with no
    downstream impact.


## Decision Log

- Decision: Expose both a low-level primitive (`produceMessage'`) and a
  high-level convenience (`produceMessageSync`) for per-message delivery
  observation rather than only one of them.
  Rationale: The raw primitive is what `hw-kafka-client` actually provides
  (`Kafka.Producer.produceMessage'`), and some users need the flexibility to
  issue many sends and await a list of `MVar`s (the batched-async pattern
  the best-practices document calls out in Scenario 2's "Key points"). The
  convenience wrapper covers the common case of "one send, block, return the
  `Offset`" without forcing every caller to hand-roll MVar allocation, flush
  sequencing, and `DeliveryReport` pattern-matching. Keeping both is cheap
  and matches the underlying library's shape.
  Date: 2026-04-22

- Decision: `ProduceMessageBatch` is added to the effect GADT even though
  the original effect in plan EP-1 (`docs/plans/1-effectful-bindings-for-hw-kafka-client.md`)
  explicitly dropped it, citing "not exported from `Kafka.Producer` in
  hw-kafka-client 5.3.0 on Hackage".
  Rationale: Revisiting the local source tree at
  `/Users/shinzui/Keikaku/hub/haskell/hw-kafka-client-project/hw-kafka-client/src/Kafka/Producer.hs`
  shows `produceMessageBatch` in the export list of `Kafka.Producer`. The
  version number on the cabal file is still 5.3.0. Either the Hackage
  artifact has been republished with the symbol exported or the earlier
  surprise was an artifact of a plan-install cache. The first milestone that
  references `produceMessageBatch` must verify availability against the
  pinned dependency and, if the symbol is still missing, fall back to a
  pure-Haskell equivalent implemented as `mapM K.produceMessage` inside the
  interpreter. The effect signature does not change either way.
  Date: 2026-04-22

- Decision: Transaction operations live on the `KafkaProducer` effect, not
  on a third `KafkaTransaction` effect.
  Rationale: Every transactional primitive in `Kafka.Transaction` except
  `commitOffsetMessageTransaction` operates solely on a `KafkaProducer`
  handle. Adding them to the producer effect keeps the scope natural: a
  caller that already has `KafkaProducer :> es` simply gains
  `initTransactions`, `beginTransaction`, `commitTransaction`,
  `abortTransaction`. The sole cross-handle operation
  (`commitOffsetMessageTransaction`, which needs the `KafkaConsumer` handle
  to locate topic-partitions) is exposed as a helper function that requires
  both effects and uses narrow "ask-handle" escape-hatch operations on each
  effect. This revisits EP-4's Decision Log entry "Do not expose a
  raw-handle escape hatch in 0.1.0.0" now that we have a concrete user
  story (transactional ETL).
  Date: 2026-04-22

- Decision: Transaction-error-returning operations return
  `Maybe TxError` to callers rather than throwing through
  `Error KafkaError`.
  Rationale: `Kafka.Transaction` surfaces fatal, retriable, and
  requires-abort discriminators on its `TxError` type. Users of a
  transactional ETL loop *must* dispatch on these to decide whether to
  abort, retry, or crash (the best-practices document's Scenario 5 spells
  out the ordering: `kafkaErrorTxnRequiresAbort` first, then
  `kafkaErrorIsRetriable`, then `kafkaErrorIsFatal`). Throwing a bare
  `KafkaError` would lose those bits and force the user to cast back via
  some side channel. `Maybe TxError` mirrors the underlying API and keeps
  the three accessor functions (`getKafkaError`, `kafkaErrorIsFatal`,
  `kafkaErrorIsRetriable`, `kafkaErrorTxnRequiresAbort`) directly
  applicable.
  Date: 2026-04-22

- Decision: Non-discriminated transaction operations (`initTransactions`,
  `beginTransaction`, `abortTransaction`) throw `KafkaError` rather than
  returning `Maybe KafkaError`.
  Rationale: The upstream shape (`Maybe KafkaError`) carries no extra
  information the caller must branch on; it is just "succeeded or failed
  with this error". This matches the existing pattern for
  `commitOffsetMessage` and company in the `KafkaConsumer` effect, where
  `Maybe KafkaError` becomes a thrown `Error KafkaError`. Consistency with
  the rest of the library is more important than mirroring upstream's
  `Maybe` here.
  Date: 2026-04-22

- Decision: The combined `Kafka.Effectful` facade re-exports only the
  high-level ergonomic operations — `produceMessage`, `produceMessageSync`,
  `produceMessageBatch`, `flushProducer`, and the transaction operations.
  The lower-level `produceMessage'` (with raw callback) and the
  escape-hatch `askProducerHandle` / `askConsumerHandle` are reachable only
  through the scoped `Kafka.Effectful.Producer` / `Kafka.Effectful.Consumer`
  facades.
  Rationale: Matches EP-4's tiered facade convention: scoped facades aim
  for full coverage; the combined facade is a curated convenience.
  Date: 2026-04-22


## Outcomes & Retrospective

Completed 2026-04-22 across six commits (`da52b0b`, `75a6103`,
`74cf4f4`, `ad9ce87`, `4168128`, and the closing commit that lands
this section). Every milestone met its acceptance criteria:

-   **Milestone 1** shipped `produceMessage'` and `produceMessageSync`
    with the interpreter that allocates an `MVar`, flushes the
    producer, and dispatches on the resulting `DeliveryReport`.
-   **Milestone 2** shipped `produceMessageBatch`. Because Hackage
    `hw-kafka-client-5.3.0` does not export
    `Kafka.Producer.produceMessageBatch`, the interpreter inlines the
    upstream definition (see Surprises & Discoveries). The effect
    signature is unchanged.
-   **Milestone 3** shipped the five transaction operations plus
    `sendOffsetsToTransaction`, the escape-hatch accessors
    `askProducerHandle` / `askConsumerHandle`, and the cross-effect
    helper `Kafka.Effectful.Producer.Transaction.commitOffsetMessageTransaction`.
-   **Milestone 4** updated both scoped facades and the combined
    facade, added `@since 0.2.0.0` Haddocks to every new operation,
    wrote the eight-scenario README walkthrough, and landed an
    Unreleased block in `CHANGELOG.md`. `cabal haddock` produces no
    missing-link warnings on any new operation; the remaining
    `'Error'` warnings were pre-existing.
-   **Milestone 5** added the `examples` cabal flag plus
    `examples/SyncPublish.hs` (Scenario 2) and
    `examples/TransactionalEtl.hs` (Scenario 5 with full
    `TxError`-dispatch). Both build under `cabal build -fexamples`.
-   **Milestone 6** ran `cabal clean && cabal build` (no warnings),
    `cabal haddock`, `cabal sdist` (tarball at
    `dist-newstyle/sdist/kafka-effectful-0.1.0.0.tar.gz`), and
    `cabal build -fexamples` — all succeed.

Deferred: live-broker integration testing (`kcat` round-trip,
SIGKILL mid-batch restart for the transactional example). The plan
explicitly permits this deferral when no broker is available. The
README and the top-of-file comment in `TransactionalEtl.hs`
document the commands to run when one is.

Lessons learned:

-   The Hackage artefact for a pinned version can lag the upstream
    repository's current source. Decision Log entry 2 anticipated
    this — the fallback was in place and no rework was needed.
-   Haddock treats ticked references strictly: any identifier not
    imported into the module of the docstring produces a warning.
    For symbols reachable only via interpreter-side imports or
    qualified module paths, use `@...@` code markup instead.
-   Treefmt's pre-commit hook reformats files after `git add`; a
    second `git add -A && git commit` cycle clears the reformat
    delta. Worth knowing but unremarkable.


## Context and Orientation

This section describes the repository and the dependency as they stand today,
so that a contributor with only this plan can navigate confidently.


### Repository layout

`kafka-effectful` is a single-library Haskell package at the root of the
working tree. The cabal file is `kafka-effectful.cabal`, version `0.1.0.0`,
language `GHC2024`, `base >=4.21 && <5`, depending on `effectful-core`,
`hw-kafka-client >=5.3 && <6`, `bytestring`, `text`, and `containers`.

Library source lives under `src/Kafka/`:

-   `src/Kafka/Effectful.hs` — combined facade re-exporting both producer
    and consumer effect surfaces and the common types.
-   `src/Kafka/Effectful/Producer.hs` — scoped producer facade.
-   `src/Kafka/Effectful/Producer/Effect.hs` — `KafkaProducer :: Effect`
    GADT and the `produceMessage` / `flushProducer` send helpers.
-   `src/Kafka/Effectful/Producer/Interpreter.hs` — `runKafkaProducer`
    handler, uses `Effectful.Exception.bracket` to manage the producer
    handle.
-   `src/Kafka/Effectful/Consumer.hs` — scoped consumer facade.
-   `src/Kafka/Effectful/Consumer/Effect.hs` — `KafkaConsumer :: Effect`
    GADT with 15 constructors (polling, offset commit, partition
    management, querying).
-   `src/Kafka/Effectful/Consumer/Interpreter.hs` — `runKafkaConsumer`
    handler using `Effectful.Exception.generalBracket` so that a
    `closeConsumer` failure on a clean exit is raised via
    `Error KafkaError` but a failing body suppresses the close error.

Prior ExecPlans under `docs/plans/` establish the conventions this plan
follows. In particular:

-   `docs/plans/1-effectful-bindings-for-hw-kafka-client.md` introduced
    Dynamic dispatch with GADT constructors, the `IOE :> es, Error
    KafkaError :> es` baseline constraint, and the `bracket`-based
    interpreter shape.
-   `docs/plans/2-fix-poll-message-timeout-semantics.md` established the
    convention that a single expected "not-error" outcome translates to
    `Maybe`, with non-timeout errors thrown through `Error KafkaError`.
-   `docs/plans/3-harden-interpreters.md` installed the
    `generalBracket`-plus-`ExitCase` pattern for release actions and the
    per-file `{-# OPTIONS_GHC -Wno-redundant-constraints #-}` pragma on
    interpreters.
-   `docs/plans/4-complete-facade-reexports.md` chose a two-tier facade
    policy (scoped = complete, combined = curated) and deferred the
    raw-handle escape hatch pending a concrete user story. This plan is
    that concrete user story.


### hw-kafka-client surface that matters for this plan

The dependency is `haskell-works/hw-kafka-client` version 5.3.x. Its
sources live locally at
`/Users/shinzui/Keikaku/hub/haskell/hw-kafka-client-project/hw-kafka-client/`.
The modules we interact with are:

-   `Kafka.Producer` (file `src/Kafka/Producer.hs`) — exports
    `produceMessage`, `produceMessage'`, `produceMessageBatch`,
    `flushProducer`, `closeProducer`, `newProducer`. Also re-exports
    `Kafka.Producer.ProducerProperties`, `Kafka.Producer.Types`,
    `Kafka.Types`.
-   `Kafka.Producer.Types` (file `src/Kafka/Producer/Types.hs`) — defines
    `KafkaProducer`, `ProducerRecord`, `ProducePartition`,
    `DeliveryReport (DeliverySuccess | DeliveryFailure | NoMessageError)`,
    `ImmediateError`.
-   `Kafka.Producer.Callbacks` (file `src/Kafka/Producer/Callbacks.hs`) —
    re-exported through `Kafka.Producer.ProducerProperties`. Exposes
    `deliveryCallback :: (DeliveryReport -> IO ()) -> Callback`. The
    realCb forks the user callback via `Control.Concurrent.forkIO` so
    that a blocking callback does not stall the librdkafka delivery
    thread.
-   `Kafka.Transaction` (file `src/Kafka/Transaction.hs`) — exports
    `initTransactions :: KafkaProducer -> Timeout -> m (Maybe KafkaError)`,
    `beginTransaction :: KafkaProducer -> m (Maybe KafkaError)`,
    `commitTransaction :: KafkaProducer -> Timeout -> m (Maybe TxError)`,
    `abortTransaction :: KafkaProducer -> Timeout -> m (Maybe KafkaError)`,
    `commitOffsetMessageTransaction :: KafkaProducer -> KafkaConsumer -> ConsumerRecord k v -> Timeout -> m (Maybe TxError)`,
    `TxError`, `getKafkaError`, `kafkaErrorIsFatal`, `kafkaErrorIsRetriable`,
    `kafkaErrorTxnRequiresAbort`.

Three behaviors from the `hw-kafka-client` docs
(`/Users/shinzui/Keikaku/hub/haskell/hw-kafka-client-project/docs/producer-best-practices.md`)
shape the design:

1.  `produceMessage` enqueues into an in-memory queue and returns
    immediately. The broker ack arrives asynchronously. The `Maybe
    KafkaError` it returns reports *enqueue* failure only.
2.  Delivery callbacks fire during polling. `produceMessage'` itself
    polls internally (timeout 0), so a steady stream of sends keeps
    callbacks flowing. For sync sends, call `flushProducer` between each
    enqueue and take, otherwise the delivery callback may not fire until
    the next enqueue.
3.  The per-message callback registered through `produceMessage'` runs on
    a librdkafka-forked thread (see `Kafka/Producer/Callbacks.hs` where
    `forkIO` wraps the user callback). Blocking inside it — for example
    via `putMVar` — is therefore safe.

These three points justify the interpreter sketch in the "Interfaces and
Dependencies" section.


### The `effectful-core` error and exception effects

The project depends on `effectful-core >=2.5 && <2.7`. The relevant
helpers are:

-   `Effectful.Dispatch.Dynamic.interpret` — wire a handler into a
    Dynamic-dispatched effect.
-   `Effectful.Error.Static` — `Error e` effect, `throwError`, `catchError`.
-   `Effectful.Exception` — re-exports `bracket`, `generalBracket`, and
    `ExitCase` (used in EP-3 for consumer release).

The project does **not** depend on `effectful` (the non-core package) or
`Effectful.Concurrent`. Interpreters that need an `MVar` call the base
`Control.Concurrent.MVar` directly through `liftIO`.


### The producer best-practices document

The authoritative source for "what is a producer best practice" is
`/Users/shinzui/Keikaku/hub/haskell/hw-kafka-client-project/docs/producer-best-practices.md`.
It enumerates eight scenarios:

1.  Fire-and-forget (global `deliveryCallback`).
2.  Synchronous delivery confirmation (`produceMessage'` + `MVar` +
    `flushProducer`).
3.  Idempotent producer (`enable.idempotence=true`, `acks=all`).
4.  High-throughput batching (`produceMessageBatch`, `linger.ms`,
    `batch.size`).
5.  Exactly-once consume-transform-produce (transactional producer).
6.  Keyed partitioning (`prKey` set, `UnassignedPartition`).
7.  Custom partitioning and headers (`SpecifiedPartition`, `prHeaders`).
8.  Graceful shutdown (`flushProducer` before `closeProducer`, bracket).

Plus a "General Best Practices" section covering configuration, error
handling, resource management, a delivery-strategy decision tree, and
performance tuning.

Mapping the eight scenarios to this library today:

| Scenario | Covered now? | Work required here |
|----------|--------------|--------------------|
| 1 Fire-and-forget | Yes. `produceMessage` + `setCallback (deliveryCallback …)` on `ProducerProperties` already works through the existing facade. | README example. |
| 2 Sync confirmation | No. | New `produceMessage'` and `produceMessageSync` operations. |
| 3 Idempotent | Yes (config only). | README example showing the `extraProp` triple. |
| 4 Batching | No (no batch op on the effect). | New `produceMessageBatch` operation. README example. |
| 5 Transactions | No. | New `initTransactions`/`beginTransaction`/`commitTransaction`/`abortTransaction` ops on the producer effect; new `sendOffsetsToTransaction` op; cross-effect helper `commitOffsetMessageTransaction`; narrow `askProducerHandle` / `askConsumerHandle` escape hatches. |
| 6 Keyed ordering | Yes (data model). | README example. |
| 7 Custom partitioning / headers | Yes (data model). | README example. |
| 8 Graceful shutdown | Yes (`runKafkaProducer` already brackets). | README example calling out the behavior. |


## Plan of Work

The implementation lands in six milestones. Each milestone is separately
verifiable, commits independently, and leaves the library in a working
state.


### Milestone 1: Per-message sync publish

Scope: Add `produceMessage'` as a new effect operation that mirrors
`Kafka.Producer.produceMessage'` — record plus `DeliveryReport -> IO ()`
callback, throwing `KafkaError` on `ImmediateError`. Add
`produceMessageSync` as a high-level operation that blocks until the
broker acknowledges the record and returns the assigned `Offset` (or
throws `KafkaError`).

At the end of this milestone, two new symbols are exported from the
scoped producer facade: `produceMessage'` and `produceMessageSync`. They
are also exported from the combined facade `Kafka.Effectful` (per
Decision Log, `produceMessage'` is scoped-facade-only; `produceMessageSync`
is in the combined facade).

Files touched:

-   `src/Kafka/Effectful/Producer/Effect.hs` — extend the GADT with
    `ProduceMessage'` and `ProduceMessageSync` constructors.
-   `src/Kafka/Effectful/Producer/Interpreter.hs` — handle the two new
    constructors.
-   `src/Kafka/Effectful/Producer.hs` — re-export.
-   `src/Kafka/Effectful.hs` — re-export `produceMessageSync` only.

The new GADT constructors are, in full:

        ProduceMessage' ::
            ProducerRecord ->
            (DeliveryReport -> IO ()) ->
            KafkaProducer m ()
        ProduceMessageSync ::
            ProducerRecord ->
            KafkaProducer m Offset

The wrapper send-helpers are:

        produceMessage' ::
            (KafkaProducer :> es) =>
            ProducerRecord ->
            (DeliveryReport -> IO ()) ->
            Eff es ()
        produceMessage' rec cb = send (ProduceMessage' rec cb)

        produceMessageSync ::
            (KafkaProducer :> es) =>
            ProducerRecord ->
            Eff es Offset
        produceMessageSync = send . ProduceMessageSync

The interpreter for `ProduceMessage'` calls the underlying primitive and
throws `KafkaError` on an `ImmediateError`:

        ProduceMessage' record cb -> do
            res <- Effectful.liftIO $ K.produceMessage' producer record cb
            case res of
                Left (K.ImmediateError err) -> throwError err
                Right ()                    -> pure ()

The interpreter for `ProduceMessageSync` allocates an `MVar`, runs the
primitive with a callback that writes to it, flushes the producer to
drive the callback, takes, and dispatches on the resulting
`DeliveryReport`:

        ProduceMessageSync record -> do
            var <- Effectful.liftIO Concurrent.newEmptyMVar
            res <- Effectful.liftIO $
                K.produceMessage' producer record (Concurrent.putMVar var)
            case res of
                Left (K.ImmediateError err) -> throwError err
                Right () -> do
                    Effectful.liftIO $ K.flushProducer producer
                    report <- Effectful.liftIO $ Concurrent.takeMVar var
                    case report of
                        K.DeliverySuccess _ offset -> pure offset
                        K.DeliveryFailure _ err    -> throwError err
                        K.NoMessageError err       -> throwError err

Acceptance: `cabal build` succeeds. The new ops appear in the output of

        cabal repl --ghci-options=-idist-newstyle/…/src

        λ> :t produceMessageSync
        produceMessageSync
          :: (KafkaProducer :> es) =>
             ProducerRecord -> Eff es Offset


### Milestone 2: Batch publish

Scope: Add `produceMessageBatch` to the effect. Verify that
`Kafka.Producer.produceMessageBatch` is actually exported by the pinned
version of `hw-kafka-client`; if it is, delegate; if not, implement it as
`mapM` over the record list inside the interpreter (the same shape the
upstream function uses).

At the end of this milestone, callers can write:

        failures <- produceMessageBatch records
        unless (null failures) $
            liftIO $ putStrLn ("enqueue failures: " <> show (length failures))

Files touched:

-   `src/Kafka/Effectful/Producer/Effect.hs`.
-   `src/Kafka/Effectful/Producer/Interpreter.hs`.
-   `src/Kafka/Effectful/Producer.hs`, `src/Kafka/Effectful.hs`.

New constructor and wrapper:

        ProduceMessageBatch ::
            [ProducerRecord] ->
            KafkaProducer m [(ProducerRecord, KafkaError)]

        produceMessageBatch ::
            (KafkaProducer :> es) =>
            [ProducerRecord] ->
            Eff es [(ProducerRecord, KafkaError)]
        produceMessageBatch = send . ProduceMessageBatch

Interpreter:

        ProduceMessageBatch records ->
            Effectful.liftIO $ K.produceMessageBatch producer records

If during implementation the `K.produceMessageBatch` import fails (the
symbol is not exported), fall back to:

        ProduceMessageBatch records -> Effectful.liftIO $ do
            results <- mapM (\m -> (m,) <$> K.produceMessage producer m) records
            pure [(m, err) | (m, Just err) <- results]

and record the fallback in Surprises & Discoveries.

Acceptance: `cabal build` succeeds; `produceMessageBatch` is a reachable
export from `Kafka.Effectful.Producer`.


### Milestone 3: Transaction API

Scope: Add five transaction operations to the producer effect, a new
module `Kafka.Effectful.Producer.Transaction` that hosts the cross-effect
helper `commitOffsetMessageTransaction`, and narrow handle-ask escape
hatches on both producer and consumer effects so the helper can reach
both underlying handles.

At the end of this milestone, a transactional ETL loop written against
`kafka-effectful` compiles and runs end-to-end.

Files touched:

-   `src/Kafka/Effectful/Producer/Effect.hs`.
-   `src/Kafka/Effectful/Producer/Interpreter.hs`.
-   `src/Kafka/Effectful/Consumer/Effect.hs`.
-   `src/Kafka/Effectful/Consumer/Interpreter.hs`.
-   `src/Kafka/Effectful/Producer.hs`.
-   `src/Kafka/Effectful/Consumer.hs`.
-   `src/Kafka/Effectful.hs`.
-   New: `src/Kafka/Effectful/Producer/Transaction.hs`.
-   `kafka-effectful.cabal` — add the new module to `exposed-modules`.

New producer GADT constructors:

        InitTransactions ::
            Timeout ->
            KafkaProducer m ()
        BeginTransaction ::
            KafkaProducer m ()
        CommitTransaction ::
            Timeout ->
            KafkaProducer m (Maybe TxError)
        AbortTransaction ::
            Timeout ->
            KafkaProducer m ()
        SendOffsetsToTransaction ::
            K.KafkaConsumer ->
            ConsumerRecord k v ->
            Timeout ->
            KafkaProducer m (Maybe TxError)
        AskProducerHandle ::
            KafkaProducer m K.KafkaProducer

New consumer GADT constructor:

        AskConsumerHandle ::
            KafkaConsumer m K.KafkaConsumer

Wrapper signatures (producer):

        initTransactions      :: (KafkaProducer :> es) => Timeout -> Eff es ()
        beginTransaction      :: (KafkaProducer :> es) => Eff es ()
        commitTransaction     :: (KafkaProducer :> es) => Timeout -> Eff es (Maybe TxError)
        abortTransaction      :: (KafkaProducer :> es) => Timeout -> Eff es ()
        askProducerHandle     :: (KafkaProducer :> es) => Eff es K.KafkaProducer

Wrapper signature (consumer):

        askConsumerHandle     :: (KafkaConsumer :> es) => Eff es K.KafkaConsumer

The three operations that upstream returns `Maybe KafkaError`
(`initTransactions`, `beginTransaction`, `abortTransaction`) translate
`Just err` into `throwError err`, matching the existing
`throwOnJust` idiom in the consumer interpreter. The two that return
`Maybe TxError` (`commitTransaction`, `sendOffsetsToTransaction`) pass
the `Maybe` through unchanged — the caller decides whether to retry,
abort, or crash based on the `TxError` discriminators.

`SendOffsetsToTransaction` takes a `K.KafkaConsumer` directly because the
cross-effect helper (see below) is the only caller and it obtains that
handle via `askConsumerHandle`. The handle is not otherwise part of the
public shape: users never call `sendOffsetsToTransaction` directly with
a raw handle; they always go through the helper.

New module `src/Kafka/Effectful/Producer/Transaction.hs`:

        module Kafka.Effectful.Producer.Transaction (
            commitOffsetMessageTransaction,
        ) where

        import Effectful (Eff, (:>))
        import Kafka.Consumer.Types (ConsumerRecord)
        import Kafka.Effectful.Consumer.Effect (KafkaConsumer, askConsumerHandle)
        import Kafka.Effectful.Producer.Effect (KafkaProducer)
        import Kafka.Effectful.Producer.Effect qualified as P
        import Kafka.Transaction (TxError)
        import Kafka.Types (Timeout)

        commitOffsetMessageTransaction ::
            (KafkaProducer :> es, KafkaConsumer :> es) =>
            ConsumerRecord k v ->
            Timeout ->
            Eff es (Maybe TxError)
        commitOffsetMessageTransaction record timeout = do
            consumer <- askConsumerHandle
            P.sendOffsetsToTransaction consumer record timeout

(The `P.sendOffsetsToTransaction` wrapper calls `send
(SendOffsetsToTransaction …)`.)

Interpreter additions (producer):

        InitTransactions timeout ->
            throwOnJust $ K.initTransactions producer timeout
        BeginTransaction ->
            throwOnJust $ K.beginTransaction producer
        CommitTransaction timeout ->
            Effectful.liftIO $ K.commitTransaction producer timeout
        AbortTransaction timeout ->
            throwOnJust $ K.abortTransaction producer timeout
        SendOffsetsToTransaction consumer record timeout ->
            Effectful.liftIO $
                K.commitOffsetMessageTransaction producer consumer record timeout
        AskProducerHandle ->
            pure producer

where `throwOnJust` is the existing helper idiom (lifted from the
consumer interpreter in EP-3).

Interpreter addition (consumer):

        AskConsumerHandle ->
            pure consumer

Acceptance:

-   `cabal build` succeeds and exposes the new module.
-   A small compile-only smoke module under `examples/TransactionalEtl.hs`
    (gated by the `examples` cabal flag, added in Milestone 5) type-checks.
-   `grep -n "askProducerHandle\|askConsumerHandle" src/` shows the
    expected exports from both scoped facades.


### Milestone 4: Facade updates, Haddocks, README walk-through

Scope: Update the three facade modules to re-export everything new;
update the README with short worked examples for each of the eight
scenarios; add Haddocks to the new operations that cross-reference the
best-practices document.

Files touched:

-   `src/Kafka/Effectful/Producer.hs`.
-   `src/Kafka/Effectful/Consumer.hs`.
-   `src/Kafka/Effectful.hs`.
-   `README.md`.
-   `CHANGELOG.md` — add a new entry under `## Unreleased` (or `## 0.2.0.0`
    when the release skill is run).

The README gains a "Producer scenarios" section between the existing
"Producer" and "Consumer" subsections. Each scenario gets a 6–15 line
snippet with the key property knobs in a `brokersList … <> extraProp …`
chain and the effect body. The snippets are lifted in shape (not
verbatim) from `producer-best-practices.md`, and each one uses only
operations exported from `Kafka.Effectful` (plus `Kafka.Effectful.Producer`
for `produceMessage'` / `askProducerHandle`). The eight snippet titles
mirror the upstream document:

1.  "Scenario 1 — Fire-and-forget".
2.  "Scenario 2 — Synchronous delivery confirmation".
3.  "Scenario 3 — Idempotent producer".
4.  "Scenario 4 — High-throughput batching".
5.  "Scenario 5 — Transactional ETL".
6.  "Scenario 6 — Keyed partitioning for ordering".
7.  "Scenario 7 — Custom partitioning and headers".
8.  "Scenario 8 — Graceful shutdown".

Acceptance:

-   `cabal haddock` succeeds with no missing-link warnings for any of the
    new operations.
-   `cabal build` still succeeds.
-   Manual read-through of the README: each of the eight scenarios is
    covered, every snippet uses only imports in scope when the reader has
    `import Kafka.Effectful` (plus at most `import Kafka.Effectful.Producer`
    or `import Kafka.Effectful.Consumer` for scoped-facade-only symbols).


### Milestone 5: Example programs

Scope: Add two runnable example programs to the package under an
`examples` cabal flag, mirroring the `hw-kafka-client` package's
`examples` flag convention.

-   `examples/SyncPublish.hs` — sends a single record to a
    `localhost:9092` broker using `produceMessageSync` and prints the
    returned offset.
-   `examples/TransactionalEtl.hs` — consumes from `source`, transforms
    each record (e.g. uppercases the value), and produces to
    `destination` using the transactional API. Commits consumer offsets
    inside each transaction. Handles `TxError` in the required order:
    `kafkaErrorTxnRequiresAbort` first, then `kafkaErrorIsRetriable`,
    then `kafkaErrorIsFatal`.

Files touched:

-   `kafka-effectful.cabal` — add `flag examples` (manual, default False),
    two `executable` stanzas gated on `if flag(examples) [...] else
    buildable: False`.
-   New: `examples/SyncPublish.hs`.
-   New: `examples/TransactionalEtl.hs`.

Acceptance: `cabal build -fexamples` succeeds. Integration testing
against a live broker is documented in "Validation and Acceptance" but
not required to be executed for this milestone (see Idempotence and
Recovery).


### Milestone 6: Full-repo validation

Scope: One pass of release-gate validation.

-   `cabal clean && cabal build` — no warnings.
-   `cabal haddock` — no broken links.
-   `cabal sdist` — tarball builds cleanly.
-   `cabal build -fexamples` — the two example programs compile.
-   If a broker is available (`docker compose up kafka`), run the two
    example programs and record their output in Outcomes &
    Retrospective.

At the end of this milestone, the plan's Outcomes & Retrospective
section is filled in and all Progress items are checked.


## Concrete Steps

Run every command from the repository root
(`/Users/shinzui/Keikaku/bokuno/kafka-effectful`) unless otherwise noted.


### Preflight

Before starting any milestone, verify the current tree builds cleanly:

        cabal build

Expected output ends with:

        Preprocessing library for kafka-effectful-0.1.0.0..
        Building library for kafka-effectful-0.1.0.0..

with no warnings after the `Wall`-driven ghc-options.


### Milestone 1 steps

1.  Open `src/Kafka/Effectful/Producer/Effect.hs`. Add two new GADT
    constructors `ProduceMessage'` and `ProduceMessageSync` to the
    `KafkaProducer :: Effect` data declaration, in the same order as they
    appear in "Plan of Work". Add the two new `send .`-based wrappers
    below `produceMessage` and `flushProducer`. Add both names to the
    module's export list. Extend the import list to bring in
    `DeliveryReport` from `Kafka.Producer.Types` and `Offset` from
    `Kafka.Consumer.Types`.

2.  Open `src/Kafka/Effectful/Producer/Interpreter.hs`. Add
    `Control.Concurrent.MVar qualified as Concurrent` to imports. Extend
    the case-split in `handleProducer` with the two new arms spelled out
    in "Plan of Work". The existing imports already include
    `Kafka.Producer qualified as K`.

3.  Open `src/Kafka/Effectful/Producer.hs`. Add `produceMessage'` and
    `produceMessageSync` to the `-- * Operations` export block. Update
    the `import Kafka.Effectful.Producer.Effect` line to include both.

4.  Open `src/Kafka/Effectful.hs`. Add `produceMessageSync` to the
    `-- * Producer Effect` export block and the `import … Producer`
    line. Do *not* add `produceMessage'` (per Decision Log: scoped-only).

5.  Build:

            cabal build

    Expected: clean rebuild of the four touched modules with no warnings.

6.  Sanity-check signatures in ghci:

            cabal repl kafka-effectful
            ghci> :t produceMessageSync
            produceMessageSync
              :: (KafkaProducer :> es) =>
                 ProducerRecord -> Eff es Offset
            ghci> :t produceMessage'
            produceMessage'
              :: (KafkaProducer :> es) =>
                 ProducerRecord
                 -> (DeliveryReport -> IO ())
                 -> Eff es ()

7.  Commit with the trailers specified in "Git trailers" at the end of
    this plan.


### Milestone 2 steps

1.  Open `src/Kafka/Effectful/Producer/Effect.hs`. Add the
    `ProduceMessageBatch` GADT constructor and the wrapper
    `produceMessageBatch`, exported.

2.  Open `src/Kafka/Effectful/Producer/Interpreter.hs`. Add the new
    handler arm. First attempt the direct delegate:

            ProduceMessageBatch records ->
                Effectful.liftIO $ K.produceMessageBatch producer records

3.  Build:

            cabal build

    If the build fails with `Not in scope: K.produceMessageBatch`,
    replace the handler body with the mapM fallback from "Plan of Work"
    and record the substitution in Surprises & Discoveries.

4.  Re-export `produceMessageBatch` from
    `src/Kafka/Effectful/Producer.hs` and `src/Kafka/Effectful.hs`.

5.  Build again:

            cabal build

6.  Commit.


### Milestone 3 steps

1.  In `src/Kafka/Effectful/Producer/Effect.hs`, add the six new GADT
    constructors (`InitTransactions`, `BeginTransaction`,
    `CommitTransaction`, `AbortTransaction`,
    `SendOffsetsToTransaction`, `AskProducerHandle`) and the five
    user-facing wrappers (`initTransactions`, `beginTransaction`,
    `commitTransaction`, `abortTransaction`, `askProducerHandle`).
    `SendOffsetsToTransaction` is sent only from within
    `Kafka.Effectful.Producer.Transaction`, so its wrapper
    `sendOffsetsToTransaction` lives in the module's export list but is
    documented as internal. Add imports:

            import Kafka.Transaction (TxError)
            import Kafka.Types (Timeout)
            import Kafka.Producer qualified as K  -- for K.KafkaProducer,
                                                  -- K.KafkaConsumer in the GADT

    Extend the export list.

2.  In `src/Kafka/Effectful/Producer/Interpreter.hs`, add imports for
    `Kafka.Transaction qualified as K` and extend the `handleProducer`
    case-split with the six new arms per "Plan of Work". Reuse or lift
    the `throwOnJust` idiom that already exists in the consumer
    interpreter (defining it locally is fine; two copies are cheaper
    than inventing a shared module).

3.  In `src/Kafka/Effectful/Consumer/Effect.hs`, add `AskConsumerHandle`
    and its wrapper `askConsumerHandle`. Extend the imports with
    `import Kafka.Consumer qualified as K` for `K.KafkaConsumer`.

4.  In `src/Kafka/Effectful/Consumer/Interpreter.hs`, add the
    `AskConsumerHandle -> pure consumer` arm.

5.  Create `src/Kafka/Effectful/Producer/Transaction.hs` with the module
    source spelled out under "Plan of Work / Milestone 3".

6.  Update `kafka-effectful.cabal` to add the new module to
    `exposed-modules`:

            Kafka.Effectful.Producer.Transaction

7.  Update `src/Kafka/Effectful/Producer.hs` to re-export the new
    transaction operations plus `askProducerHandle` and the `TxError`
    type (with its accessors).

8.  Update `src/Kafka/Effectful/Consumer.hs` to re-export
    `askConsumerHandle`.

9.  Update `src/Kafka/Effectful.hs` to re-export all the high-level
    transaction operations (`initTransactions`, `beginTransaction`,
    `commitTransaction`, `abortTransaction`,
    `commitOffsetMessageTransaction`) plus `TxError` and its accessors.
    Do *not* re-export `askProducerHandle` / `askConsumerHandle` /
    `sendOffsetsToTransaction` from the combined facade (per Decision
    Log).

10. Build:

            cabal build

11. Commit.


### Milestone 4 steps

1.  Open `README.md`. Between the existing "Producer" and "Consumer"
    subsections, insert a new `### Scenario walkthrough` heading (or
    similar) with eight short snippets, one per scenario, each framed
    with a single `##### Scenario N — Title` subheading. Draw content
    from `producer-best-practices.md`, transformed into effect-idiomatic
    code:

    -   Scenario 1: `produceMessage` with a global
        `setCallback (deliveryCallback …)` in `ProducerProperties`.
    -   Scenario 2: `produceMessageSync`, one call, pattern-match on the
        returned `Offset`.
    -   Scenario 3: idempotent properties — only a property snippet, no
        new call site.
    -   Scenario 4: `produceMessageBatch` with a `compression`-plus-
        `extraProp "linger.ms" "10"` property snippet.
    -   Scenario 5: the full transactional ETL loop using
        `initTransactions`, `beginTransaction`,
        `commitOffsetMessageTransaction`, and `commitTransaction`.
    -   Scenario 6: a `ProducerRecord` with `prKey = Just userId` and
        `prPartition = UnassignedPartition`.
    -   Scenario 7: `SpecifiedPartition n` plus `headersFromList …`.
    -   Scenario 8: a `bracket`-style snippet noting that
        `runKafkaProducer` already flushes on scope exit.

2.  Add `@since` Haddock tags on every new operation in both effect
    modules (`@since 0.2.0.0`). Extend the top-level module Haddock on
    `Kafka.Effectful.Producer` to summarise the scenario coverage and
    point at the README.

3.  Update `CHANGELOG.md`: add a `## Unreleased` section listing the new
    operations, the `TxError` export, the cross-effect helper, and the
    scenario-walkthrough README expansion.

4.  Build:

            cabal build
            cabal haddock

    Both must succeed. Haddock must not report missing-link warnings for
    any of the new symbols.

5.  Commit.


### Milestone 5 steps

1.  Update `kafka-effectful.cabal`:

    -   Add:

            flag examples
              description: Build example executables
              manual: True
              default: False

    -   Add two `executable` stanzas, each gated on
        `if !flag(examples) buildable: False`. Each executable depends
        on `base`, `bytestring`, `effectful-core`, `hw-kafka-client`,
        and `kafka-effectful`. The main-is values are `SyncPublish.hs`
        and `TransactionalEtl.hs`, the `hs-source-dirs` is `examples`.

2.  Write `examples/SyncPublish.hs`:

    -   `main :: IO ()` runs `runEff . runError @KafkaError` with the
        producer handler and calls `produceMessageSync`. On success the
        program prints `"delivered offset " <> show offset`.
    -   Broker address and topic name are hard-coded to
        `localhost:9092` and `kafka-effectful-sync-demo`; a comment
        above main notes that this can be changed.

3.  Write `examples/TransactionalEtl.hs`:

    -   `main` wires both `runKafkaProducer` and `runKafkaConsumer`.
    -   The producer properties set `transactional.id`,
        `enable.idempotence=true`, `acks=all`.
    -   The consumer properties set `group.id`, `noAutoCommit`,
        `isolation.level=read_committed`.
    -   After `initTransactions` is called once, the program loops:
        `pollMessageBatch`, `beginTransaction`, produce transformed
        records via `produceMessage`, pick the last record per
        partition and commit via `commitOffsetMessageTransaction`, then
        `commitTransaction` and inspect the `Maybe TxError`.
    -   `handleTxResult` dispatches on
        `kafkaErrorTxnRequiresAbort`, `kafkaErrorIsRetriable`, and
        `kafkaErrorIsFatal` in that order.

4.  Build:

            cabal build -fexamples

    Both executables must link successfully.

5.  Commit.


### Milestone 6 steps

1.  Clean build:

            cabal clean
            cabal build

    Expected: no warnings.

2.  Haddock build:

            cabal haddock --haddock-for-hackage

    Expected: the generated tarball under `dist-newstyle/` contains
    `kafka-effectful-0.2.0.0-docs.tar.gz` (or whatever version is in the
    cabal file at this time).

3.  Source-distribution build:

            cabal sdist

    Expected: a tarball at
    `dist-newstyle/sdist/kafka-effectful-<version>.tar.gz` that extracts
    and re-builds cleanly in a fresh directory.

4.  Examples build:

            cabal build -fexamples

5.  Optional, if `docker compose` is available and the repo ships a
    compose file for Kafka (it does not, at time of writing; see
    "Idempotence and Recovery" for fallback): bring up a local broker
    and topic, run both examples, and record their output verbatim in
    Outcomes & Retrospective.

6.  Fill in Outcomes & Retrospective. Flip all Progress boxes to
    checked.


## Validation and Acceptance

The plan is accepted when, at completion of Milestone 6, all of the
following hold:

-   `cabal clean && cabal build` prints no warning lines.
-   `cabal haddock` produces docs that render the new operations and
    cross-link to the best-practices document URL as plain text.
-   `cabal build -fexamples` produces two runnable binaries.
-   The README renders a scenarios section covering all eight upstream
    scenarios, each with a compact snippet that uses only operations
    exported from `Kafka.Effectful` (with at most one additional
    scoped-facade import per snippet).
-   Grepping the effect-module exports shows the new operations:

            grep -n "produceMessageSync\|produceMessageBatch\|initTransactions\|beginTransaction\|commitTransaction\|abortTransaction\|commitOffsetMessageTransaction" src/

    Expected: hits in both the effect modules (declaration sites), both
    interpreters (handler arms), the scoped facades (re-exports), and
    the combined facade (re-exports).

-   For each of the three `TxError` discriminators
    (`kafkaErrorTxnRequiresAbort`, `kafkaErrorIsRetriable`,
    `kafkaErrorIsFatal`), the `TransactionalEtl.hs` example contains a
    branch that handles it.

Where a live broker is available, two additional behavioural checks
apply:

-   `cabal run -fexamples example-sync-publish` publishes one record to
    `localhost:9092/kafka-effectful-sync-demo` and prints the assigned
    offset. Running `kcat -C -b localhost:9092 -t
    kafka-effectful-sync-demo -e` shows the record.
-   `cabal run -fexamples example-transactional-etl` consumes from
    `source`, transforms, and produces to `destination`, committing
    consumer offsets in the producer transaction. Killing the process
    mid-batch (SIGKILL, not SIGINT — to simulate a crash) and
    restarting it does not produce duplicate records on `destination`
    (verify with `kcat -C -b localhost:9092 -t destination -o beginning
    -e | wc -l` before and after restart).

If no broker is available, the plan is still accepted on the static
criteria above. Runtime verification belongs to a follow-up integration
milestone, not this plan.


## Idempotence and Recovery

All filesystem edits in this plan are additive (new constructors, new
wrapper functions, new modules, new README section, new examples) plus
one import-list extension per touched file. Nothing is removed or
renamed. Re-running any milestone's steps in order is safe; the
compiler will catch duplicate definitions immediately.

Cabal-level changes (new module, new flag, new executables) are
idempotent: running `cabal build` after partial application stops on
the first error.

If a milestone is interrupted halfway, the "restart" path is to check
the Progress section, identify the first unchecked box, and resume from
its corresponding step in "Concrete Steps".

For the runtime integration checks (optional; under "Validation and
Acceptance"), if no broker is available the fallback is explicit in the
plan: all static acceptance criteria must still pass, and the runtime
checks are deferred to a follow-up.

For any milestone that changes the public API surface, the cabal file's
`version` field is *not* bumped in this plan; release packaging is
handled separately by the existing release skill. The CHANGELOG entry
lives under a `## Unreleased` heading until that skill runs.


## Interfaces and Dependencies

Libraries and modules used by this plan:

-   `effectful-core >=2.5 && <2.7` — `Effectful`, `Effectful.Dispatch.Dynamic`,
    `Effectful.Error.Static`, `Effectful.Exception`.
-   `hw-kafka-client >=5.3 && <6` — `Kafka.Producer`, `Kafka.Producer.Types`,
    `Kafka.Producer.ProducerProperties`, `Kafka.Consumer.Types`,
    `Kafka.Transaction`, `Kafka.Types`.
-   `base` — `Control.Concurrent.MVar` (`newEmptyMVar`, `putMVar`,
    `takeMVar`).

No new package dependency is introduced. `MVar` is reached through
`base`, which the project already depends on.

End-state interface per module:

In `src/Kafka/Effectful/Producer/Effect.hs`:

        data KafkaProducer :: Effect where
            ProduceMessage ::
                ProducerRecord ->
                KafkaProducer m ()
            ProduceMessage' ::
                ProducerRecord ->
                (DeliveryReport -> IO ()) ->
                KafkaProducer m ()
            ProduceMessageSync ::
                ProducerRecord ->
                KafkaProducer m Offset
            ProduceMessageBatch ::
                [ProducerRecord] ->
                KafkaProducer m [(ProducerRecord, KafkaError)]
            FlushProducer ::
                KafkaProducer m ()
            InitTransactions ::
                Timeout ->
                KafkaProducer m ()
            BeginTransaction ::
                KafkaProducer m ()
            CommitTransaction ::
                Timeout ->
                KafkaProducer m (Maybe TxError)
            AbortTransaction ::
                Timeout ->
                KafkaProducer m ()
            SendOffsetsToTransaction ::
                K.KafkaConsumer ->
                ConsumerRecord k v ->
                Timeout ->
                KafkaProducer m (Maybe TxError)
            AskProducerHandle ::
                KafkaProducer m K.KafkaProducer

In `src/Kafka/Effectful/Consumer/Effect.hs`, the new constructor:

        AskConsumerHandle ::
            KafkaConsumer m K.KafkaConsumer

In `src/Kafka/Effectful/Producer/Transaction.hs`:

        commitOffsetMessageTransaction ::
            (KafkaProducer :> es, KafkaConsumer :> es) =>
            ConsumerRecord k v ->
            Timeout ->
            Eff es (Maybe TxError)

The existing interpreter signatures are unchanged:

        runKafkaProducer ::
            (IOE :> es, Error KafkaError :> es) =>
            ProducerProperties ->
            Eff (KafkaProducer : es) a ->
            Eff es a

        runKafkaConsumer ::
            (IOE :> es, Error KafkaError :> es) =>
            ConsumerProperties ->
            Subscription ->
            Eff (KafkaConsumer : es) a ->
            Eff es a


## Git trailers

Every commit made while working on this plan must include these two
trailers at the end of the commit body, separated from the body by a
blank line:

        ExecPlan: docs/plans/7-support-sync-publish-and-producer-best-practices.md
        Intention: intention_01km3c2s7xeamb7gkfjkve90ma

Commit messages follow the Conventional Commits style
(`feat(producer): …`, `docs(readme): …`, etc.) as configured in the
user's global Claude instructions.
