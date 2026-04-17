# Prepare kafka-effectful 0.1 Release

Intention: intention_01km3c2s7xeamb7gkfjkve90ma

This MasterPlan is a living document. The sections Progress, Surprises & Discoveries,
Decision Log, and Outcomes & Retrospective must be kept up to date as work proceeds.

This document is maintained in accordance with `claude/skills/master-plan/MASTERPLAN.md`.


## Vision & Scope

After this initiative is complete, the `kafka-effectful` library at version `0.1.0.0`
is a pleasant, robust, and publishable Effectful wrapper around `hw-kafka-client`.
A Haskell developer can add `KafkaProducer :> es` or `KafkaConsumer :> es` to an
`Eff` computation, consume Kafka topics in a straightforward loop without having to
pattern-match timeouts on every poll, rely on the interpreter to clean up the
underlying `librdkafka` handle under all error paths, and publish the package to
Hackage with a conforming sdist (category, changelog, extra-source-files, README).

In scope:

- Revising the consumer polling API so that timeout is not conflated with error.
- Hardening both interpreters against resource leaks on asynchronous exceptions and
  making the `pausePartitions` / `resumePartitions` error check pattern-match on
  `RdKafkaRespErrNoError` instead of `toEnum 0`.
- Suppressing the upstream `-Wredundant-constraints` warning locally so a user
  building the library sees a clean compile.
- Deciding and implementing how much of hw-kafka-client's `ConsumerProperties` /
  `ProducerProperties` builder surface to re-export, and whether to ship a raw
  handle escape hatch for advanced users (transactions, metadata). The default
  disposition is to re-export the full builder surface and to document (not expose)
  the raw handle.
- Release packaging: adding `CHANGELOG.md`, `extra-source-files`, selecting a
  Hackage-conforming `category` value, and tightening the `effectful-core` bound
  style.
- Documentation polish: marking the experimental lifecycle clearly in the README,
  adding a complete `runEff` / `runError` example, and updating README snippets
  to reflect any API changes made during this initiative.

Out of scope:

- Transaction API, metadata queries, topic management, and the Dump API. These
  remain deferred per the original scoping in
  `docs/plans/1-effectful-bindings-for-hw-kafka-client.md`.
- Typed encoding of `ProducerRecord` keys/values. The library stays a thin
  effectful wrapper over hw-kafka-client's `ByteString`-level record type.
- OpenTelemetry-traced interpreter variants and integration test suites with a
  live Kafka broker. Both are post-0.1.


## Decomposition Strategy

The initiative decomposes into five child ExecPlans. The guiding principles were
(a) group by functional concern, (b) keep each plan independently verifiable,
(c) minimize cross-plan coupling in shared files, and (d) sequence the single
breaking API change first so downstream docs can land against the final shape.

The functional concerns:

- **Consumer polling ergonomics (EP-2).** The one breaking API change required
  before 0.1: `pollMessage` must distinguish "timed out, poll again" from real
  errors. This is user-visible and isolated to one operation.

- **Interpreter robustness (EP-3).** A cluster of small correctness fixes living
  entirely inside the two `Interpreter.hs` files and the cabal file:
  bracket-based handle acquisition that closes an async-exception window,
  pause/resume helper using a direct `RdKafkaRespErrNoError` pattern match,
  close-error handling during cleanup that does not mask the original failure,
  and scoped suppression of the known `-Wredundant-constraints` warning.

- **Facade re-export completeness (EP-4).** Re-export the remaining
  `ConsumerProperties` and `ProducerProperties` builders so typical users never
  import `hw-kafka-client` directly, and decide (and record) whether to expose a
  raw-handle escape hatch for transactions or defer it.

- **Release packaging (EP-5).** `CHANGELOG.md`, `extra-source-files`, Hackage
  category, and `effectful-core` bound style. Purely about what Hackage will see.

- **Documentation polish (EP-6).** README updates: an explicit experimental
  lifecycle notice, a full working example including `runEff . runError`, and
  the revised `pollMessage` usage after EP-2 lands.

Alternatives considered and rejected:

- **One big plan covering everything.** Rejected because interpreter robustness,
  API semantics, packaging, and docs each have different acceptance criteria and
  would drown in a single checklist.

- **Merging EP-2 with EP-3.** Rejected because EP-2 is a breaking API change with
  user-facing acceptance (a polling loop compiles and does not catch on timeouts),
  while EP-3 is a pile of internal fixes with no API surface. Keeping them separate
  lets EP-3 land even if EP-2 needs more design discussion.

- **Merging EP-5 and EP-6.** Rejected because packaging is a file-manipulation
  plan (cabal, new files) while docs is a content-writing plan. Different modes of
  work.

- **Splitting EP-3 per fix (four micro-plans).** Rejected because the fixes all
  live in the same two interpreter files and coordinating four plans on the same
  two files creates merge churn. One plan, four small milestones is cleaner.


## Exec-Plan Registry

| # | Title | Path | Hard Deps | Soft Deps | Status |
|---|-------|------|-----------|-----------|--------|
| EP-2 | Fix consumer polling timeout semantics | docs/plans/2-fix-poll-message-timeout-semantics.md | None | None | Complete |
| EP-3 | Harden producer and consumer interpreters | docs/plans/3-harden-interpreters.md | None | None | Complete |
| EP-4 | Complete facade re-exports | docs/plans/4-complete-facade-reexports.md | None | None | Complete |
| EP-5 | Finalize release packaging metadata | docs/plans/5-finalize-release-packaging.md | None | None | Complete |
| EP-6 | Polish README and release docs | docs/plans/6-polish-readme-and-docs.md | None | EP-2 | Complete |

Status values: Not Started, In Progress, Complete, Cancelled.


## Dependency Graph

EP-2, EP-3, EP-4, and EP-5 have no hard dependencies on each other. They touch
different concerns (consumer API, interpreter internals, facade exports, cabal
metadata) and, modulo the integration points listed below, can proceed in any
order or in parallel.

EP-6 has a soft dependency on EP-2. EP-6 updates the README's consumer example
to illustrate the new `pollMessage` return shape. If EP-6 lands before EP-2, the
README will show the old shape and require a follow-up; if it lands after, no
follow-up is needed. The recommendation is to do EP-6 last so the README reflects
final shapes everywhere.

There are no integration dependencies (interfaces that two plans both define and
must reconcile). Integration concerns are structural file overlap, covered in
Integration Points.

Recommended order for a single contributor working sequentially:
EP-3 → EP-2 → EP-4 → EP-5 → EP-6. This puts the breaking API change in the
middle of a sequence where the interpreter has already been tidied, reduces the
number of times the README is edited, and ends with a single release-hygiene
pass (packaging + docs).


## Integration Points

The following files are touched by more than one child plan. Each plan must be
aware of the others' edits to avoid accidental reverts.

- `src/Kafka/Effectful/Consumer/Effect.hs` — **EP-2** changes the return type
  of the `PollMessage` GADT constructor and the `pollMessage` wrapper. No other
  plan modifies this file. Owner: EP-2.

- `src/Kafka/Effectful/Consumer/Interpreter.hs` — **EP-2** changes the
  `PollMessage` handler to return `Maybe` and translate timeout without
  throwing. **EP-3** changes the pause/resume handler to use a direct
  `RdKafkaRespErrNoError` pattern, restructures the `bracket` that acquires the
  consumer handle, and changes cleanup behavior so close-errors do not mask
  action errors. Both plans add or remove small helpers at the top of the
  handler. Convention: EP-3 introduces a `throwOnKafkaErr` helper near the
  existing `throwOnJust` / `throwOnLeft`; EP-2 leaves those helpers alone and
  edits only the `PollMessage` case. Owner of structure: EP-3.

- `src/Kafka/Effectful/Producer/Interpreter.hs` — **EP-3** only. Restructures
  `bracket` to close the async-exception window and adds the scoped
  `-Wno-redundant-constraints` option.

- `src/Kafka/Effectful/Consumer.hs` — **EP-2** updates Haddocks on
  `pollMessage` that mention timeout behavior. **EP-4** extends the `K.foo`
  re-export block with the missing `ConsumerProperties` builders. The blocks
  are separated (operations vs. configuration), so the two plans do not collide
  on the same import lines if ordinary care is taken.

- `src/Kafka/Effectful/Producer.hs` — **EP-4** only. Extends the `K.foo`
  re-export block with any missing `ProducerProperties` builders.

- `src/Kafka/Effectful.hs` — **EP-2** (Haddock only, on `pollMessage` if the
  combined facade's export list gains a note); **EP-4** may extend the
  combined re-export list if new symbols are exposed in the scoped facades.

- `kafka-effectful.cabal` — **EP-3** adds a scoped `ghc-options:
  -Wno-redundant-constraints` stanza (or per-file pragma) for the interpreter
  modules. **EP-5** adds `extra-source-files: README.md CHANGELOG.md LICENSE`,
  revises `category:`, and tightens the `effectful-core` bound style. The two
  stanzas do not overlap; both plans should re-read the file before editing to
  avoid accidental conflict on adjacent lines.

- `README.md` — **EP-2** updates the consumer example to show the new
  `pollMessage :: ... Eff es (Maybe ...)` signature. **EP-6** adds the
  experimental-lifecycle note, adds a full `runEff . runError` wiring example,
  and cross-checks all examples against the final public API. Recommendation:
  run EP-6 after EP-2 so EP-6's author is the only one who needs to think about
  README consistency.

- `docs/plans/1-effectful-bindings-for-hw-kafka-client.md` — historical
  reference only. Not edited by any child plan here.


## Progress

- [x] EP-2: Breaking API change applied — `pollMessage` returns `Maybe`
- [x] EP-2: Consumer facade and combined facade Haddocks updated
- [x] EP-2: README consumer example updated (coordinated with EP-6)
- [x] EP-3: Consumer and producer interpreter `bracket` acquisition closes the async-exception window
- [x] EP-3: Pause/resume use `RdKafkaRespErrNoError` pattern match via shared helper
- [x] EP-3: `closeConsumer` error no longer masks action errors during cleanup
- [x] EP-3: Scoped `-Wno-redundant-constraints` applied; `cabal build` is clean
- [x] EP-4: Missing `ConsumerProperties` builders re-exported
- [x] EP-4: Missing `ProducerProperties` builders re-exported
- [x] EP-4: Decision on raw-handle escape hatch recorded (and, if positive, implemented)
- [x] EP-5: `CHANGELOG.md` created with a `0.1.0.0` entry
- [x] EP-5: `extra-source-files` added to the cabal file
- [x] EP-5: `category:` revised to a Hackage-conforming value
- [x] EP-5: `effectful-core` bound style tightened
- [x] EP-6: Experimental lifecycle note added to README
- [x] EP-6: Full `runEff . runError` example added to README
- [x] EP-6: All README examples type-check against the final API


## Surprises & Discoveries

- EP-2: `RdKafkaRespErrT (..)` is re-exported from `Kafka.Consumer` in
  hw-kafka-client 5.3.x, so the first import variant worked without falling
  back to `Kafka.Types` or `Kafka.Internal.RdKafka`. (2026-04-16)

- EP-3: `Effectful.Exception` (effectful-core 2.6) already re-exports
  `generalBracket` and `ExitCase(..)` (sourced from `Control.Monad.Catch`),
  so closing the async-exception window and preserving user errors over
  close errors required no new dependency. (2026-04-16)

- EP-4: The facade gap was smaller than the plan assumed — only three
  generic callback helpers (`errorCallback`, `logCallback`, `statsCallback`)
  were missing from the scoped facades. Every other `ConsumerProperties` /
  `ProducerProperties` symbol was already re-exported. (2026-04-16)

- EP-5: `cabal check` flagged two issues the plan did not anticipate: a
  `doc-place` warning recommending `extra-doc-files:` over
  `extra-source-files:` for README/CHANGELOG, and a `no-repository` warning
  asking for a `source-repository` stanza. Both were addressed; the plan
  delivered one milestone beyond its original scope as a result. Lesson:
  run `cabal check` *first* when writing release-packaging plans.
  (2026-04-16)

- EP-6: By the time EP-6 ran, the README's Consumer snippet already showed
  the `Maybe`-returning `pollMessage` shape — EP-2 (or a companion edit at
  the time of its landing) had already updated it. Milestone 3 became a
  verification rather than an edit. The recommendation in the Integration
  Points section to "run EP-6 after EP-2 so EP-6's author is the only one
  who needs to think about README consistency" still held, just in a weaker
  form than expected. (2026-04-16)


## Decision Log

- Decision: Decompose into five child ExecPlans grouped by functional concern
  rather than by file or by review finding.
  Rationale: Review findings numbered #1–#13 in the review each touch different
  concerns. Grouping by concern yields independently verifiable outcomes and
  minimizes file-level coupling across plans.
  Date: 2026-04-16

- Decision: Recommend the sequential order EP-3 → EP-2 → EP-4 → EP-5 → EP-6.
  Rationale: EP-3 is internal-only and has no risk of being reworked by later
  plans. EP-2 ships the breaking API change so EP-6 can document it once. EP-4
  and EP-5 are independent and order does not matter; placing them between EP-2
  and EP-6 keeps the final README pass as the last action. None of the hard
  dependencies are changed by this recommendation — it is guidance, not a
  constraint.
  Date: 2026-04-16

- Decision: Default disposition on the raw-handle escape hatch is "defer, do
  not expose" unless EP-4 discovers a concrete use case.
  Rationale: Transactions and metadata were explicitly deferred in the original
  bindings plan. Exposing a raw handle now commits the library to a stability
  surface that the 0.1 release does not otherwise guarantee. EP-4 records the
  final decision in its own Decision Log.
  Date: 2026-04-16


## Outcomes & Retrospective

All five child ExecPlans landed as Complete, and `kafka-effectful-0.1.0.0`
is ready to publish: `cabal check` is clean, `cabal sdist` produces a
tarball containing README, CHANGELOG, and LICENSE, and `cabal build`
compiles with no warnings from any module in the package.

What shipped, by concern:

- **Consumer API (EP-2).** `pollMessage` now returns `Maybe (ConsumerRecord
  (Maybe ByteString) (Maybe ByteString))`. `KafkaResponseError
  RdKafkaRespErrTimedOut` maps to `Nothing`; all other errors continue to
  throw through `Error KafkaError`. `pollMessageBatch` was deliberately
  left unchanged.

- **Interpreter robustness (EP-3).** Both interpreters acquire their
  `librdkafka` handles inside `Exception.bracket`'s acquisition action,
  closing the async-exception window. `runKafkaConsumer` now uses
  `Effectful.Exception.generalBracket` so a user exception from the scoped
  action is preserved over any subsequent `closeConsumer` failure.
  `PausePartitions` / `ResumePartitions` route through a shared
  `throwOnKafkaErr` helper that pattern-matches on `RdKafkaRespErrNoError`
  instead of `toEnum 0`. Both interpreter modules carry a per-file
  `{-# OPTIONS_GHC -Wno-redundant-constraints #-}` pragma with an
  explanatory comment.

- **Facade re-exports (EP-4).** Scoped facades
  (`Kafka.Effectful.Producer`, `Kafka.Effectful.Consumer`) now re-export
  `errorCallback`, `logCallback`, and `statsCallback` under their
  "Callbacks" sections. The combined `Kafka.Effectful` facade was
  deliberately not extended — it remains a curated surface. The raw-handle
  escape hatch was finalized as **Option A: defer, do not expose** for
  0.1.0.0; adding `askProducer` / `askConsumer` later is a one-line GADT
  extension in a patch release when transactions or metadata work begins.

- **Release packaging (EP-5).** `CHANGELOG.md` exists with a `0.1.0.0 —
  2026-04-16` entry marking the release experimental. The cabal file
  declares `extra-doc-files: README.md, CHANGELOG.md`,
  `category: Network, Messaging`,
  `effectful-core >=2.5 && <2.7`, and a `source-repository head` stanza
  pointing at the GitHub remote.

- **Documentation polish (EP-6).** The README opens with an
  experimental-lifecycle blockquote immediately after the one-line summary,
  and gains a "Running it" subsection with a paste-ready `main` wiring the
  Producer through `runEff . runError @KafkaError . runKafkaProducer props`.
  Every identifier used in the README was cross-checked against the
  combined facade's exports.

Comparison against the original Vision & Scope: every in-scope item
landed. Out-of-scope items (transactions, metadata, topic management, the
Dump API, typed encoding, OpenTelemetry variants, live-broker integration
tests) remain deferred to post-0.1, consistent with the original
scoping.

Deviations from plan:

- The recommended sequential order was EP-3 → EP-2 → EP-4 → EP-5 → EP-6.
  The actual implementation order (per git log) was EP-2 → EP-3 → EP-4 →
  EP-5 → EP-6 — EP-2 landed first. This swap had no practical impact: no
  child plan blocked another, and the README was still updated exactly
  once for the `pollMessage` shape change.

- EP-5 delivered one milestone beyond its original four (the
  `source-repository head` stanza), prompted by a `cabal check` warning
  the plan had not predicted.

- EP-6 Milestone 3 was a verification, not an edit, because the README
  had already been brought in line with the `Maybe`-returning
  `pollMessage` when EP-2 landed.

Lessons for future release-prep initiatives:

- Tool-driven checklists beat memory-driven ones. Running `cabal check`
  at plan-drafting time surfaces packaging gaps the plan author would
  otherwise miss.

- Enumerate the current state of a facade surface before scoping a
  "complete re-exports" plan. EP-4's research pass found the starting
  state closer to complete than the plan's Context section had
  suggested; a two-minute enumeration up front would have right-sized
  the plan.

- When an initiative contains both a breaking API change and a
  documentation plan, the API-change author tends to touch the examples
  in the README at the same time out of necessity. That is fine — the
  documentation plan's role becomes verification plus additive polish,
  not rework.


## Revision History

- 2026-04-16: Reconciliation update. All five child ExecPlans were
  implemented across commits `abb432f` (EP-2) through `c010a26` (EP-6),
  but the MasterPlan was not updated in-session. This revision marks
  every child Complete in the Exec-Plan Registry, ticks all 17 Progress
  items, populates Surprises & Discoveries with cross-plan findings
  extracted from each child plan's retrospective, and writes the
  Outcomes & Retrospective section from the landed work. No
  Decision Log entries were added in this revision — the original
  decomposition decisions stand as recorded.
