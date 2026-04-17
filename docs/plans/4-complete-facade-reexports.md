# Complete facade re-exports

MasterPlan: docs/masterplans/1-prepare-kafka-effectful-0-1-release.md
Intention: intention_01km3c2s7xeamb7gkfjkve90ma

This ExecPlan is a living document. The sections Progress, Surprises & Discoveries,
Decision Log, and Outcomes & Retrospective must be kept up to date as work proceeds.

This document is maintained in accordance with `.claude/skills/exec-plan/PLANS.md`.


## Purpose / Big Picture

Today, the scoped facades `Kafka.Effectful.Producer` and `Kafka.Effectful.Consumer`
re-export *some* of hw-kafka-client's property builders (for example,
`brokersList`, `autoCommit`, `groupId`, `compression`) but not all of them. A
typical user who only imports `Kafka.Effectful` or one of the scoped facades
has to fall back to `import qualified Kafka.Producer.ProducerProperties as K` or
`import qualified Kafka.Consumer.ConsumerProperties as K` to set builders such
as `queuedMaxMessagesKBytes`, topic timeouts, or `statisticsInterval` that are
not currently re-exported. This defeats the "you should not need to import
hw-kafka-client directly for typical use" promise stated in the library's
original design goals.

After this plan, every public builder exported by
`Kafka.Producer.ProducerProperties` and `Kafka.Consumer.ConsumerProperties` is
re-exported from the corresponding scoped facade, so a single `import
Kafka.Effectful.Producer` or `import Kafka.Effectful.Consumer` suffices for all
common configuration. The combined facade `Kafka.Effectful` is updated to match.

This plan also records a decision on the "raw handle escape hatch" question
(whether to expose a way to obtain the underlying `K.KafkaProducer` /
`K.KafkaConsumer` for advanced users who need transactions or metadata). The
default disposition, per the MasterPlan, is to defer; this plan will either
confirm the deferral by writing a Decision Log entry and a small README note,
or, if a concrete user story emerges, implement an `askProducer` / `askConsumer`
operation and add it to the effect GADT.

How to see it working: a user writing an application that needs
`queuedMaxMessagesKBytes 1024` can add `import Kafka.Effectful.Consumer` and
call `queuedMaxMessagesKBytes 1024` without any other import. The Haddocks for
both scoped facades list every configuration builder that hw-kafka-client ships
for the corresponding properties type.


## Progress

- [x] Enumerate all public symbols exported from `Kafka.Producer.ProducerProperties` in the installed version of hw-kafka-client (5.3.x). _2026-04-16._
- [x] Enumerate all public symbols exported from `Kafka.Consumer.ConsumerProperties` in the same version. _2026-04-16._
- [x] Identify which symbols are already re-exported by the scoped facades and which are missing. _2026-04-16._
- [x] Add the missing producer builder re-exports to `src/Kafka/Effectful/Producer.hs`. _2026-04-16._
- [x] Add the missing consumer builder re-exports to `src/Kafka/Effectful/Consumer.hs`. _2026-04-16._
- [x] Update `src/Kafka/Effectful.hs` if any newly-exported symbol should also appear in the combined facade. _2026-04-16 — intentionally not updated; see Decision Log._
- [x] Record the decision on the raw-handle escape hatch in the Decision Log and, if implementing, add it to the effect and interpreter. _2026-04-16 — Option A (defer) confirmed; no code changes._
- [x] Run `cabal build` and confirm clean compile. _2026-04-16._
- [x] Write Outcomes & Retrospective. _2026-04-16._


## Surprises & Discoveries

- The producer and consumer property modules in hw-kafka-client 5.3.x already
  re-export `module Kafka.Producer.Callbacks` / `module Kafka.Consumer.Callbacks`
  from their own export lists, which in turn re-export `module Kafka.Callbacks`.
  Because the scoped facades import `Kafka.Producer.ProducerProperties qualified
  as K` (respectively the consumer variant), the three generic callback helpers
  (`errorCallback`, `logCallback`, `statsCallback`) are reachable as
  `K.errorCallback` etc. without any additional import — they simply weren't
  listed in the facade export lists.

- Every non-callback public symbol from the two property modules was already
  re-exported before this plan. The only gap against the "re-export everything"
  stance was the three generic callback helpers described above. No esoteric
  builder was missed; the starting state was closer to complete than the plan's
  "Context and Orientation" section suggested.

- Evidence: after the edits, `cabal build` reports a clean rebuild of
  `Kafka.Effectful.Producer`, `Kafka.Effectful.Consumer`, and `Kafka.Effectful`
  with no warnings about unused qualified imports.


## Decision Log

- Decision: Scoped facades aim for full coverage; the combined facade
  `Kafka.Effectful` remains a curated convenience re-export focused on the
  most common symbols.
  Rationale: Users who want a thin import (`import Kafka.Effectful`) will
  rarely need every esoteric builder. Users who want complete coverage import
  the scoped facade directly. This mirrors the library's current two-tier
  facade structure.
  Date: 2026-04-16

- Decision: On the raw-handle escape hatch — default is "defer, do not
  expose" unless research in Milestone 3 surfaces a concrete blocker.
  Rationale: Exposing the raw `K.KafkaProducer` / `K.KafkaConsumer` couples
  the library's stability surface to hw-kafka-client's internal handle type,
  which is reasonable for a thin wrapper but should be a deliberate choice,
  not an accident. The original plan explicitly deferred transactions and
  metadata. If a user needs them today, they can (per the current
  documentation gap) fall back to constructing and managing the handle
  themselves outside of the effect. The final decision is recorded here at
  the end of Milestone 3.
  Date: 2026-04-16 (default; may be revised by Milestone 3)

- Decision (final): Option A — do not expose a raw-handle escape hatch in
  0.1.0.0. No `askProducer` / `askConsumer` operation is added; the effect
  GADTs and interpreters are unchanged by this plan.
  Rationale: No concrete user story was surfaced during this plan's work or
  in the open sibling plans (EP-5 release packaging, EP-6 README). The
  library has not shipped, so no external user can yet report a blocker.
  Adding the operation is a one-line GADT extension plus `pure handle` in
  the interpreter; it is cheap to add in a patch release if and when
  transactions or metadata support land. Shipping the escape hatch now
  would commit us to the shape of the returned handle type as part of the
  0.1 public API before we know what advanced operations will actually
  want. README coverage of this gap is owned by EP-6.
  Date: 2026-04-16

- Decision: The combined facade `Kafka.Effectful` is not updated by this
  plan. The only newly-exported scoped-facade symbols are the three generic
  callback helpers `errorCallback`, `logCallback`, `statsCallback`.
  Rationale: The combined facade today exports no property builders or
  callback constructors — it focuses on effect operations, properties
  records, consumer/producer types, subscription builders, and common
  types. Adding three callback helpers in isolation (without also promoting
  `brokersList`, `groupId`, `deliveryCallback`, etc.) would be
  inconsistent. Users who want callback-based configuration already need
  to reach for the scoped facade to get `deliveryCallback` / `setCallback`
  / `rebalanceCallback`; they can pick up the generic three there as well.
  This preserves the "selective stance in the combined facade" recorded in
  the earlier Decision Log entry.
  Date: 2026-04-16


## Outcomes & Retrospective

Scope delivered:

- `Kafka.Effectful.Producer` now re-exports `errorCallback`, `logCallback`,
  and `statsCallback` under the "Callbacks" section heading. The full set of
  `Kafka.Producer.ProducerProperties` public exports plus the full set of
  `Kafka.Producer.Callbacks` re-exports (which chains through
  `Kafka.Callbacks`) is now re-exported from the scoped facade.
- `Kafka.Effectful.Consumer` similarly gains `errorCallback`, `logCallback`,
  and `statsCallback`. All other public exports of
  `Kafka.Consumer.ConsumerProperties` (including `queuedMaxMessagesKBytes`,
  `callbackPollMode`, and the consumer callback constructors) were already
  re-exported and required no change.
- `Kafka.Effectful` (combined facade) was intentionally not modified; see the
  Decision Log for why.
- The raw-handle escape hatch decision is finalized as Option A (defer) for
  the 0.1.0.0 release. No effect GADT or interpreter change was made.

Verification:

- `cabal build` compiles cleanly with the same warning set as before (the
  `-Wredundant-constraints` notes on `handleConsumer` / `handleProducer`
  were already silenced by EP-3, per commit 39d542c on this branch).
- A user wanting the runtime-callback form of configuration can now write
  `setCallback (errorCallback myHandler)` with only
  `import Kafka.Effectful.Consumer` or `import Kafka.Effectful.Producer`.

Gaps and follow-ups:

- The README does not yet document that transactions and metadata
  operations require dropping down to hw-kafka-client directly. That
  documentation is owned by EP-6 (`docs/plans/6-polish-readme-and-docs.md`)
  and should cite the Option-A decision recorded above.
- If a 5.3 patch release of hw-kafka-client adds a new builder, this plan's
  "re-export everything in the scoped facade" stance requires a one-line
  follow-up; no structural change is expected.

Lessons learned:

- The original plan overestimated the gap. A short enumeration pass before
  writing the plan would have reduced it to a single-file, three-line
  change per facade. That said, the enumeration itself was the bulk of the
  work: verifying the `module Kafka.Producer.Callbacks` / `module
  Kafka.Consumer.Callbacks` re-export chains was necessary to be confident
  that `errorCallback` et al. were the only missing symbols.
- For the combined facade, the right default when unsure was "leave it
  alone" rather than "add a partial set". Partial additions lead to
  surprising imports down the line.


## Context and Orientation

The library already uses a two-tier facade:

- `src/Kafka/Effectful.hs` is the combined facade. It re-exports the
  producer and consumer effects and a curated list of common types. It does
  *not* re-export property builders today (with a small exception for
  `brokersList` implied via the scoped facades).

- `src/Kafka/Effectful/Producer.hs` and `src/Kafka/Effectful/Consumer.hs`
  are the scoped facades. Each re-exports its effect, interpreter, common
  types, and a selection of property builders via the `K.foo` idiom, where
  `K` is a qualified import of the corresponding hw-kafka-client module.

Currently, `Kafka.Effectful.Producer` re-exports (from
`Kafka.Producer.ProducerProperties`):

    K.brokersList
    K.setCallback
    K.logLevel
    K.compression
    K.topicCompression
    K.sendTimeout
    K.statisticsInterval
    K.extraProps
    K.extraProp
    K.suppressDisconnectLogs
    K.extraTopicProps
    K.debugOptions
    K.deliveryCallback
    K.Callback

And `Kafka.Effectful.Consumer` re-exports (from
`Kafka.Consumer.ConsumerProperties`):

    K.brokersList
    K.autoCommit
    K.noAutoCommit
    K.noAutoOffsetStore
    K.groupId
    K.clientId
    K.setCallback
    K.logLevel
    K.compression
    K.suppressDisconnectLogs
    K.statisticsInterval
    K.extraProps
    K.extraProp
    K.debugOptions
    K.queuedMaxMessagesKBytes
    K.callbackPollMode
    K.rebalanceCallback
    K.offsetCommitCallback
    K.Callback

hw-kafka-client 5.3.x source lives at
`/Users/shinzui/Keikaku/hub/haskell/hw-kafka-client-project/hw-kafka-client/src`.
The two files to enumerate are:

- `src/Kafka/Producer/ProducerProperties.hs`
- `src/Kafka/Consumer/ConsumerProperties.hs`

Read each module's export list (the parentheses after `module X (`) to
identify every public symbol. Compare to the lists above. Every symbol not
already in the scoped facade must be added.

Do not assume the module exports only what is mentioned in this plan. The
hw-kafka-client authors may add new builders in a 5.3 patch release, and
the "re-export everything" stance should survive that.

For the `K.Callback` newtype and the callback constructors (`deliveryCallback`,
`rebalanceCallback`, `offsetCommitCallback`), they are already re-exported
where relevant. If the enumeration surfaces additional callback constructors,
re-export them too.

The MasterPlan at `docs/masterplans/1-prepare-kafka-effectful-0-1-release.md`
states that this plan does not conflict with any other plan in the initiative
except at shared files (`src/Kafka/Effectful.hs`, `src/Kafka/Effectful/Consumer.hs`).
EP-2 edits Haddocks in those files and the `PollMessage` shape, and EP-6 edits
the README. None overlap with builder re-exports on the same lines, but all
three plans should re-read the affected files before editing to avoid mid-plan
drift.


## Plan of Work

Three milestones. Milestones 1 and 2 can be done in either order; milestone 3
is optional research and may be collapsed to a single Decision Log entry.

**Milestone 1: Complete producer builder re-exports.**

Scope: every public symbol from `Kafka.Producer.ProducerProperties` appears
in the `Kafka.Effectful.Producer` export list.

Work:

1. Read `/Users/shinzui/Keikaku/hub/haskell/hw-kafka-client-project/hw-kafka-client/src/Kafka/Producer/ProducerProperties.hs`.
   List every identifier in the `module` export list.

2. Compare to the existing `K.foo` re-exports in
   `src/Kafka/Effectful/Producer.hs` (see the "Context and Orientation"
   section above for the current list).

3. Add any missing identifier as a `K.<name>` line in the scoped facade's
   export list, grouped under an appropriate `-- *` section heading. Keep
   sections tidy:

        -- * Configuration
        ProducerProperties (..),
        K.brokersList,
        K.setCallback,
        ...

        -- * Callbacks
        K.deliveryCallback,
        K.Callback,

4. If any identifier is a type or data constructor (not a function), add it
   to the export list with the appropriate `(..)` as needed.

5. `cabal build` and confirm no unused-import warnings arise from the
   `Kafka.Producer.ProducerProperties qualified as K` import.

**Milestone 2: Complete consumer builder re-exports.**

Scope: same as milestone 1 but for
`Kafka.Consumer.ConsumerProperties` and
`src/Kafka/Effectful/Consumer.hs`.

Work:

1. Read `/Users/shinzui/Keikaku/hub/haskell/hw-kafka-client-project/hw-kafka-client/src/Kafka/Consumer/ConsumerProperties.hs`.
   List every identifier in the `module` export list.

2. Compare to the existing `K.foo` re-exports (see Context).

3. Add any missing identifier to the export list, grouped under the
   appropriate `-- *` heading.

4. `cabal build` and confirm.

**Milestone 3: Decide on the raw-handle escape hatch.**

Scope: produce a written decision. Implement if the decision is positive.

Options:

- **Option A (default): defer.** Record in the Decision Log that raw-handle
  access is not exposed in 0.1.0.0. Add a short paragraph to the README (or
  ensure EP-6 does) stating that transactions and metadata operations are
  not yet supported and require direct hw-kafka-client usage.

- **Option B: expose.** Add two operations to the relevant effect GADTs:

        AskProducer :: KafkaProducer m K.KafkaProducer
        AskConsumer :: KafkaConsumer m K.KafkaConsumer

  with corresponding wrappers:

        askProducer :: (KafkaProducer :> es) => Eff es K.KafkaProducer
        askProducer = send AskProducer

        askConsumer :: (KafkaConsumer :> es) => Eff es K.KafkaConsumer
        askConsumer = send AskConsumer

  Implement them in the two interpreters as `pure handle`. Re-export both
  from the scoped facades and (probably) from the combined facade. Note in
  Haddock that the returned handle is only valid for the duration of the
  surrounding `runKafkaProducer` / `runKafkaConsumer` scope.

Decision procedure: choose Option A unless (a) an explicit user request for
transactions or metadata is found in the repository history or open plans,
or (b) a concrete use case known to the implementer requires escape-hatch
access before 0.1 ships. Record the chosen option in the Decision Log with
a short rationale.


## Concrete Steps

Enumeration commands, run from the repository root:

    rg -n '^module Kafka.Producer.ProducerProperties' /Users/shinzui/Keikaku/hub/haskell/hw-kafka-client-project/hw-kafka-client/src/Kafka/Producer/ProducerProperties.hs

followed by a quick visual read of the export parentheses block in that
file. Do the same for
`/Users/shinzui/Keikaku/hub/haskell/hw-kafka-client-project/hw-kafka-client/src/Kafka/Consumer/ConsumerProperties.hs`.

After each edit to a facade file:

    cabal build

Expected success output: the library compiles with the same warning set as
before this plan (the `-Wredundant-constraints` warnings on
`handleConsumer` / `handleProducer`, unless EP-3 has already suppressed
them).


## Validation and Acceptance

The plan is complete when:

1. For every symbol in the export list of
   `Kafka.Producer.ProducerProperties`, the same symbol is either
   re-exported from `Kafka.Effectful.Producer` or explicitly skipped with
   a reason noted in Surprises & Discoveries.

2. The same property holds for `Kafka.Consumer.ConsumerProperties` and
   `Kafka.Effectful.Consumer`.

3. `cabal build` succeeds and `cabal haddock` (optional) produces Haddocks
   in which every re-exported identifier appears under the correct section
   heading.

4. The Decision Log contains an entry recording the Option A / Option B
   choice for the raw-handle escape hatch, and if Option B was chosen, the
   two new effect operations are implemented, interpreted, and re-exported.


## Idempotence and Recovery

All edits are additive to module export lists. Revert with `git restore
<path>` on any file. The enumeration step is idempotent. If a later
hw-kafka-client patch release adds a builder this plan did not re-export,
a one-line follow-up adds it; no design change is needed.


## Interfaces and Dependencies

After milestones 1 and 2, the public exports of `Kafka.Effectful.Producer`
and `Kafka.Effectful.Consumer` grow; no existing export is removed or
renamed. If milestone 3 adopts Option B, the public exports also include:

    askProducer :: (KafkaProducer :> es) => Eff es K.KafkaProducer
    askConsumer :: (KafkaConsumer :> es) => Eff es K.KafkaConsumer

where `K.KafkaProducer` and `K.KafkaConsumer` are the opaque handle types
from hw-kafka-client. Users must only use these within the scope of the
effect's interpreter.

Dependencies used:

- `hw-kafka-client` >= 5.3 && < 6 — source of truth for the builder APIs.
- `effectful-core` ^>= 2.5 || ^>= 2.6 — only relevant if milestone 3
  implements Option B.
