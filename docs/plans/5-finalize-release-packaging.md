# Finalize release packaging metadata

MasterPlan: docs/masterplans/1-prepare-kafka-effectful-0-1-release.md
Intention: intention_01km3c2s7xeamb7gkfjkve90ma

This ExecPlan is a living document. The sections Progress, Surprises & Discoveries,
Decision Log, and Outcomes & Retrospective must be kept up to date as work proceeds.

This document is maintained in accordance with `.claude/skills/exec-plan/PLANS.md`.


## Purpose / Big Picture

Before `kafka-effectful-0.1.0.0` can be uploaded to Hackage as a well-formed
candidate, a handful of packaging details must be addressed:

- There is no `CHANGELOG.md` at the repository root. Hackage displays a
  package's changelog prominently, and `cabal sdist` emits a warning when one
  is absent. Every 0.x library should ship with at least an initial entry.

- The cabal file has no `extra-source-files:` stanza. Without it, `cabal
  sdist` does not include `README.md`, `CHANGELOG.md`, or `LICENSE` in the
  source tarball (well, `LICENSE` is handled via `license-file:` already, but
  README and CHANGELOG are not). Hackage will render the README on the package
  page only if it is in the tarball.

- `category: Kafka` is not a recognized Hackage category. The recognized
  categories are a fixed list (see
  `https://hackage.haskell.org/packages/` category index); `Kafka` alone is
  nonstandard. A conforming value is `Network, Messaging` or
  `Distributed Computing, Messaging`.

- The cabal bound on `effectful-core` currently reads
  `^>=2.5 || ^>=2.6`. This is legal but unusual; the two caret bounds are
  both upper-open on the same minor-major, so the combined range is
  `>= 2.5.0 && < 2.7.0`. Writing that directly is clearer.

After this plan, `cabal sdist` produces a tarball that contains `README.md`,
`CHANGELOG.md`, and `LICENSE`; the cabal file declares a Hackage-recognized
category; and the `effectful-core` bound reads as a single conventional range
instead of two alternatives.

How to see it working: running `cabal sdist` from the repository root
produces `dist-newstyle/sdist/kafka-effectful-0.1.0.0.tar.gz`, and
`tar tf` on the tarball lists `kafka-effectful-0.1.0.0/README.md`,
`kafka-effectful-0.1.0.0/CHANGELOG.md`, and
`kafka-effectful-0.1.0.0/LICENSE`. `cabal check` reports no errors.


## Progress

- [x] Create `CHANGELOG.md` at the repository root with an initial `0.1.0.0` entry. (2026-04-16)
- [x] Add docs stanza to `kafka-effectful.cabal` listing README and CHANGELOG. Landed as `extra-doc-files:` (not `extra-source-files:`) per cabal's own `doc-place` hint; LICENSE remains handled by `license-file:`. (2026-04-16)
- [x] Revise `category:` in the cabal file to a Hackage-recognized value — `Network, Messaging`. (2026-04-16)
- [x] Revise the `effectful-core` build-depends bound to a single range — `>=2.5 && <2.7`. (2026-04-16)
- [x] Add `source-repository head` stanza to silence `cabal check`'s `no-repository` warning (out-of-plan addition, see Surprises). (2026-04-16)
- [x] Run `cabal check` and confirm no errors or warnings. (2026-04-16)
- [x] Run `cabal sdist` and verify the tarball includes `README.md`, `CHANGELOG.md`, and `LICENSE`. (2026-04-16)
- [x] Write Outcomes & Retrospective. (2026-04-16)


## Surprises & Discoveries

- `cabal check` emitted two warnings on the first pass that the plan did
  not anticipate:
  1. `[doc-place]` recommending `CHANGELOG.md` be moved from
     `extra-source-files:` to `extra-doc-files:`. Cabal treats docs-y
     filenames as documentation-class extras; the `extra-doc-files:`
     stanza is the modern home for them.
  2. `[no-repository]` asking for at least one `source-repository`
     stanza. The git remote was already configured
     (`git@github.com:shinzui/kafka-effectful.git`), so the data was
     there — only the cabal declaration was missing.
  Both warnings were addressed (see Decision Log) and `cabal check`
  now reports "No errors or warnings could be found in the package."

- The plan's original validation item 3 checked for an
  `extra-source-files:` stanza. After landing, the equivalent
  validation is a check for the `extra-doc-files:` stanza; the plan's
  Validation section has been updated accordingly.


## Decision Log

- Decision: Use `category: Network, Messaging` as the Hackage category.
  Rationale: The library is a network client for a messaging system; both
  categories are standard Hackage category values. Listing both gives the
  package the right indexing on hackage.haskell.org. Alternatives considered:
  `Distributed Computing, Messaging` (accurate but less discoverable);
  `Network, Kafka` (rejected because `Kafka` alone is not a standard
  category).
  Date: 2026-04-16

- Decision: The initial changelog entry is dated with today's date
  (`2026-04-16`) and marks the 0.1.0.0 release as experimental.
  Rationale: Honest about lifecycle; the package's `mori.dhall` declares
  `Lifecycle.Experimental`, and shipping a 0.1.0.0 on Hackage should not
  hide that. Users reading the changelog immediately see "expect breaking
  changes".
  Date: 2026-04-16

- Decision: Use `extra-doc-files:` (not `extra-source-files:`) for
  README and CHANGELOG.
  Rationale: `cabal check` explicitly emitted a `doc-place` warning
  asking that CHANGELOG move to `extra-doc-files:`. Rather than
  splitting — README under one stanza, CHANGELOG under another — both
  documentation files go under `extra-doc-files:`, which is the
  modern cabal convention for package docs. Both still end up in the
  sdist tarball, which is what Hackage needs to render them.
  Alternatives considered: keeping the plan's literal
  `extra-source-files:` spelling (rejected — leaves a spurious
  warning on every future `cabal check`); splitting across both
  stanzas (rejected — asymmetric for no gain).
  Date: 2026-04-16

- Decision: Add a `source-repository head` stanza pointing to
  `https://github.com/shinzui/kafka-effectful.git` with `type: git`.
  Rationale: `cabal check` warned `no-repository`. The data was not
  in question (the git remote was already set), only the cabal
  declaration. Adding the stanza also gives Hackage a "Source
  repository" link on the package page, which users expect on an
  open-source library. This is a small scope addition beyond the
  plan's original four milestones; recorded here and reflected in
  Progress.
  Date: 2026-04-16


## Outcomes & Retrospective

Outcome: `kafka-effectful-0.1.0.0`'s release metadata is complete.
`cabal check` reports no errors or warnings. `cabal sdist` produces
`dist-newstyle/sdist/kafka-effectful-0.1.0.0.tar.gz`, which contains
`kafka-effectful-0.1.0.0/README.md`,
`kafka-effectful-0.1.0.0/CHANGELOG.md`, and
`kafka-effectful-0.1.0.0/LICENSE` — the three files Hackage needs
to render a well-formed package page.

Changes landed:

- `CHANGELOG.md` at the repository root with a `0.1.0.0 — 2026-04-16`
  entry marking the release as experimental.
- `kafka-effectful.cabal` now declares
  `extra-doc-files: README.md, CHANGELOG.md`, a
  `source-repository head` pointing at the GitHub remote,
  `category: Network, Messaging`, and
  `effectful-core >=2.5 && <2.7`.

Against the original purpose: the four issues the plan called out are
all fixed. The plan met its acceptance bar (`cabal check` clean,
tarball listing includes all three metadata files) and went one
milestone beyond — the `source-repository` stanza — because
`cabal check` surfaced a warning the plan did not anticipate.

Lessons:

- Run `cabal check` *first* when writing a release-packaging plan,
  not only at the end. Two of the four issues this plan fixed were
  named by the plan author from memory; cabal itself would have
  named a third (`no-repository`) immediately. Using the tool's own
  diagnostics as the plan's checklist would have produced a more
  complete plan on the first draft.

- `extra-doc-files:` is the modern cabal spelling for README and
  CHANGELOG. `extra-source-files:` still works but draws a
  `doc-place` warning for docs-class filenames. Future plans
  touching cabal metadata should use `extra-doc-files:` from the
  outset.


## Context and Orientation

The repository root is `/Users/shinzui/Keikaku/bokuno/kafka-effectful`. The
current cabal file is `kafka-effectful.cabal`. Its top declaration reads:

    cabal-version: 3.4
    name:          kafka-effectful
    version:       0.1.0.0
    synopsis:      Effectful effects for hw-kafka-client
    description:
      Effectful effects and interpreters for hw-kafka-client, a Haskell
      binding to Apache Kafka via librdkafka. Provides typed, composable
      KafkaProducer and KafkaConsumer effects.

    license:       MIT
    license-file:  LICENSE
    author:        Nadeem Bitar
    maintainer:    Nadeem Bitar
    category:      Kafka
    build-type:    Simple

The `build-depends` list inside `library` currently includes:

    , effectful-core   ^>=2.5 || ^>=2.6

There is no `extra-source-files:` stanza anywhere in the file.

The following files already exist at the repository root and should be
packaged into the sdist:

- `README.md` — top-level user documentation.
- `LICENSE` — MIT, already referenced via `license-file:`.

The following file does **not** yet exist and must be created by this plan:

- `CHANGELOG.md`.

Hackage categories: the canonical index lives on hackage.haskell.org and
includes entries such as `Network`, `Distributed Computing`, `Messaging`,
`Concurrency`, `Data`, etc. Multiple categories are comma-separated. The
Haskell category system is informal but widely understood; using a
standard pair avoids orphaning the package in an index of one.

The MasterPlan at `docs/masterplans/1-prepare-kafka-effectful-0-1-release.md`
notes that this plan and EP-3 both edit `kafka-effectful.cabal`. EP-3 adds a
per-file `{-# OPTIONS_GHC -Wno-redundant-constraints #-}` pragma inside
`.hs` files rather than a cabal stanza, so under the current EP-3 design
there is no overlap in the cabal file. If EP-3's decision changes,
re-read `kafka-effectful.cabal` before editing it here.


## Plan of Work

Four small milestones.

**Milestone 1: Create `CHANGELOG.md`.**

Scope: a `CHANGELOG.md` exists at the repository root with one entry for
`0.1.0.0`.

File content:

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

If the initiative includes other child plans that change user-visible
behavior (for example, EP-2 changes the `pollMessage` return shape), mention
those changes here. Since this plan runs as part of the 0.1.0.0 release and
no prior Hackage release exists, the changelog can reasonably summarize the
entire initiative's outcome as "Initial release".

**Milestone 2: Add `extra-source-files` to the cabal file.**

Scope: `cabal sdist` includes README and CHANGELOG in the tarball.

Edit `kafka-effectful.cabal`. After the `build-type: Simple` line and before
the `common warnings` stanza, add:

    extra-source-files:
      README.md
      CHANGELOG.md

`LICENSE` is already included via `license-file:` so it does not need to
be listed here.

**Milestone 3: Revise `category:`.**

Scope: `category:` is a recognized Hackage value.

Edit `kafka-effectful.cabal`. Change:

    category:      Kafka

to:

    category:      Network, Messaging

**Milestone 4: Tighten the `effectful-core` bound.**

Scope: the bound is a single range.

Edit `kafka-effectful.cabal`. Change:

    , effectful-core   ^>=2.5 || ^>=2.6

to:

    , effectful-core   >=2.5 && <2.7

The semantic range is the same. If upstream later releases `2.7` and this
library has been tested against it, bump the upper bound in a follow-up.


## Concrete Steps

Run the following commands from the repository root
(`/Users/shinzui/Keikaku/bokuno/kafka-effectful`):

    cabal check

Expected output on success (no errors or warnings):

    No errors or warnings could be found in the package.

    cabal sdist

Expected output excerpt:

    Wrote tarball sdist to /Users/shinzui/Keikaku/bokuno/kafka-effectful/dist-newstyle/sdist/kafka-effectful-0.1.0.0.tar.gz

    tar tf dist-newstyle/sdist/kafka-effectful-0.1.0.0.tar.gz | sort

Expected output (sorted, partial):

    kafka-effectful-0.1.0.0/CHANGELOG.md
    kafka-effectful-0.1.0.0/LICENSE
    kafka-effectful-0.1.0.0/README.md
    kafka-effectful-0.1.0.0/kafka-effectful.cabal
    kafka-effectful-0.1.0.0/src/Kafka/Effectful.hs
    ...

Confirm all three of `README.md`, `CHANGELOG.md`, and `LICENSE` appear in
the listing.


## Validation and Acceptance

The plan is complete when all of the following hold:

1. `CHANGELOG.md` exists at the repository root and contains a `0.1.0.0`
   entry.
2. `grep -n '^category:' kafka-effectful.cabal` reports a
   Hackage-recognized value, not `Kafka` alone.
3. `grep -n '^extra-doc-files' kafka-effectful.cabal` reports the
   stanza with `README.md` and `CHANGELOG.md` entries. (Updated from
   `extra-source-files` after `cabal check`'s `doc-place` hint — see
   Surprises & Discoveries.)
4. `grep -n 'effectful-core' kafka-effectful.cabal` shows the
   `>=2.5 && <2.7` bound.
5. `cabal check` exits with code 0 and prints no warnings.
6. `cabal sdist` succeeds and the tarball listing contains all three
   metadata files.


## Idempotence and Recovery

All edits are additive or purely textual. Revert with `git restore <path>`.
The `cabal sdist` output is deterministic — re-run as many times as
needed.


## Interfaces and Dependencies

No source code changes. The cabal file and repository root gain
metadata.

Dependencies used:

- `cabal-install` — to run `cabal check` and `cabal sdist`. Already
  available in the development environment per `flake.nix`.
