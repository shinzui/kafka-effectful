---
id: 6
slug: polish-readme-and-docs
title: "Polish README and release docs"
kind: exec-plan
created_at: 2026-04-17T03:34:04Z
intention: "intention_01km3c2s7xeamb7gkfjkve90ma"
master_plan: "docs/masterplans/1-prepare-kafka-effectful-0-1-release.md"
---


# Polish README and release docs

This ExecPlan is a living document. The sections Progress, Surprises & Discoveries,
Decision Log, and Outcomes & Retrospective must be kept up to date as work proceeds.

This document is maintained in accordance with `.claude/skills/exec-plan/PLANS.md`.


## Purpose / Big Picture

Today's README at the repository root gives a terse, accurate, but incomplete
picture of the library. Two gaps matter for a 0.1 Hackage release:

- It does not call out that the library is experimental. The package's
  `mori.dhall` declares `Lifecycle.Experimental`, but a user reading the
  README does not see that warning before they depend on it. Users of a
  0.1.0.0 package on Hackage deserve a plain statement that breaking changes
  are expected.

- The example snippets show only the effect bodies (`runKafkaProducer props
  $ do ...`) with a type signature that mentions `Error KafkaError :> es`
  but do not show where `runEff` and `runError` actually live. A user new to
  effectful will read the example and reasonably ask "what goes in `main`?"
  A full working example closes that loop.

This plan updates the README to include an experimental lifecycle note, a
complete `main`-level wiring example using `runEff` and `runError`, and —
as a cross-check — re-verifies that every snippet aligns with the final
public API after EP-2, EP-3, EP-4, and EP-5 have landed. This plan has a
soft dependency on EP-2 for the `pollMessage` shape; it is intended to be
the last plan in the initiative.

How to see it working: the README at the repository root opens with a clear
lifecycle note, the Producer and Consumer examples each show a complete
`main` function that a reader could paste into a file and compile (given a
running Kafka broker), and every identifier mentioned in the examples is
exported by the library.


## Progress

- [x] Add an "Experimental" lifecycle note near the top of `README.md`. (2026-04-16)
- [x] Add a complete `main`-level wiring example using `runEff . runError`. (2026-04-16)
- [x] Update the Consumer example to show the `pollMessage :: ... Eff es (Maybe ...)` shape from EP-2. Already landed before this plan ran; the README's existing Consumer snippet matches EP-2's signature. (2026-04-16)
- [x] Cross-check every identifier mentioned in the README against the final export list of `Kafka.Effectful`. (2026-04-16)
- [x] Confirm the README still compiles mentally for a reader by reading it end-to-end. (2026-04-16)
- [x] Write Outcomes & Retrospective. (2026-04-16)


## Surprises & Discoveries

- When this plan began, the Consumer snippet in `README.md` already used the
  `Maybe`-returning `pollMessage` shape and included the trailing prose
  paragraph explaining `Nothing` semantics. EP-2 (or a companion edit) had
  already touched the README, so Milestone 3 was a verification rather than
  an edit. Evidence: `README.md` at the start of this plan already contained
  the `case mbMsg of Nothing -> loop; Just msg -> ...` pattern.
  Date: 2026-04-16


## Decision Log

- Decision: The lifecycle note is a short paragraph near the top of the
  README, immediately after the one-line summary, not a badge or a callout
  box.
  Rationale: A plain sentence is readable in any Markdown renderer
  (including terminal renderers like `bat` and the Hackage HTML renderer).
  Badges are optional decoration; the warning must not depend on image
  rendering.
  Date: 2026-04-16

- Decision: The `main`-level example shows one wiring pattern
  (`runEff . runError @KafkaError . runKafkaProducer props`) rather than
  a catalog of options.
  Rationale: The README is for orientation, not reference. A single clear
  pattern does more for a new user than three alternatives.
  Date: 2026-04-16


## Outcomes & Retrospective

Outcome: `README.md` now opens with an experimental-status blockquote
immediately after the one-line summary, retains the original Producer and
Consumer snippets, and gains a new "Running it" subsection that shows a
complete `main` wiring the Producer through `runEff . runError @KafkaError`
with a `{-# LANGUAGE TypeApplications #-}` pragma and the explicit imports
from `Effectful` and `Effectful.Error.Static`.

Identifier cross-check against `src/Kafka/Effectful.hs`: every function and
type name appearing in the README (`runKafkaProducer`, `produceMessage`,
`flushProducer`, `runKafkaConsumer`, `pollMessage`, `commitOffsetMessage`,
`Timeout`, `OffsetCommit`, `KafkaError`, `ProducerProperties`,
`ProducerRecord`) is exported from the `Kafka.Effectful` facade. `runEff`,
`IOE`, `Eff`, `Error`, and `runError` come from `effectful`/`Effectful.Error.Static`
and are correctly imported in the example.

Comparison against purpose: both gaps called out in the Purpose section are
closed. A reader is now warned about the experimental lifecycle on the first
screen and has a paste-ready `main` that shows where `runEff` and `runError`
live. Milestone 3's concern about the `pollMessage` shape was already
satisfied in the tree when this plan ran, so no edit was required there.

Lessons: when a documentation plan is scheduled last in an initiative, an
earlier plan may have already touched the documentation in passing. Reading
the current file before starting saved a redundant edit here.


## Context and Orientation

The current README at `/Users/shinzui/Keikaku/bokuno/kafka-effectful/README.md`
contains:

- A one-line summary ("Effectful effects and interpreters for ...").
- A one-line reference to `effectful`.
- A "Features" bullet list.
- A "Usage" section with a Producer snippet and a Consumer snippet.
- A "Module Structure" table.
- A "Requirements" section.
- A one-line license note.

The current Producer snippet:

    import Kafka.Effectful

    example :: (IOE :> es, Error KafkaError :> es) => Eff es ()
    example =
      runKafkaProducer producerProps $ do
        produceMessage record
        flushProducer

The current Consumer snippet:

    import Kafka.Effectful

    example :: (IOE :> es, Error KafkaError :> es) => Eff es ()
    example =
      runKafkaConsumer consumerProps subscription $ do
        msg <- pollMessage (Timeout 1000)
        commitOffsetMessage OffsetCommit msg

Both have the two issues described in Purpose: no `main` wiring, and the
Consumer example is wrong under EP-2 (it binds `msg` as if `pollMessage`
returns `ConsumerRecord` directly, not `Maybe ConsumerRecord`).

Effectful handlers that must appear in `main`:

- `runEff :: Eff '[IOE] a -> IO a` — discharges the IO effect and
  returns the result. Lives in the `Effectful` module.
- `runError :: forall e es a. Eff (Error e : es) a -> Eff es (Either (CallStack, e) a)`
  — discharges the `Error` effect. Lives in `Effectful.Error.Static`.

A typical `main` using both:

    main :: IO ()
    main = do
      result <- runEff . runError @KafkaError $ runProgram
      case result of
        Left (_, err) -> putStrLn $ "Kafka error: " <> show err
        Right ()      -> pure ()
      where
        runProgram = runKafkaProducer producerProps $ do
          produceMessage record
          flushProducer

The `@KafkaError` type application requires the `TypeApplications`
extension at the call site. Alternative without type applications:

    result <- runEff (runError runProgram :: Eff '[IOE] (Either (CallStack, KafkaError) ()))

but `TypeApplications` is the idiomatic effectful style.

The library's default extensions (declared in the cabal file) do not
include `TypeApplications`. A README example that uses `@KafkaError`
should include an explicit `{-# LANGUAGE TypeApplications #-}` pragma at
the top of the imagined file, or the pragma should be mentioned in
surrounding prose.

The MasterPlan at `docs/masterplans/1-prepare-kafka-effectful-0-1-release.md`
sets this plan to run last. If, contrary to that recommendation, this plan
runs before EP-2, the Consumer snippet retains the old `pollMessage` shape
and a Progress item here notes "revisit after EP-2".

`producerProps`, `consumerProps`, `subscription`, and `record` in the
examples are intentionally symbolic. A reader is expected to fill them in
from the hw-kafka-client property builders (for example,
`brokersList [BrokerAddress "localhost:9092"] <> ...`). The README does
not need to show a full broker configuration — that is out of scope for
a 40-line orientation document.


## Plan of Work

Three milestones. Milestone 3 depends on EP-2 having landed.

**Milestone 1: Experimental lifecycle note.**

Scope: a reader opening `README.md` sees, within the first screen, a clear
statement that the library is experimental.

Edit `README.md`. After the initial summary paragraph (the one that reads
"Effectful effects and interpreters for ..."), insert a new paragraph:

    > **Status: experimental.** This package is on its first release. The
    > API may change in breaking ways in subsequent 0.x versions. Pin to an
    > exact version in production until 1.0 is tagged.

The blockquote style is readable in plain Markdown, on Hackage, and in
terminal renderers. A single paragraph keeps the top of the README quick
to scan.

**Milestone 2: Complete `main` wiring example.**

Scope: a reader can copy a full working program (minus the specific broker
configuration) into a file, add a `main-is:` entry to an executable
stanza, and compile it.

Add a new README subsection after "Usage" titled "Running it". Content:

    ### Running it

    The effect handlers `runKafkaProducer` and `runKafkaConsumer` require
    `IOE` and `Error KafkaError` in the effect stack. A complete program
    wires them with `runEff` and `runError`:

        {-# LANGUAGE TypeApplications #-}

        import Effectful
        import Effectful.Error.Static (runError)
        import Kafka.Effectful

        main :: IO ()
        main = do
          result <- runEff . runError @KafkaError $ runProgram
          case result of
            Left (_, err) -> putStrLn ("Kafka error: " <> show err)
            Right ()      -> pure ()
          where
            runProgram =
              runKafkaProducer producerProps $ do
                produceMessage record
                flushProducer

    Replace `producerProps` and `record` with your own
    `ProducerProperties` and `ProducerRecord` values (see the
    `Kafka.Effectful.Producer` module for the available builders).

Format the code block per the existing README conventions — the existing
file uses fenced Markdown code blocks with `haskell` syntax tagging.

**Milestone 3: Align Consumer example with the final API.**

Scope: the Consumer snippet in "Usage" uses the new `pollMessage :: ...
Eff es (Maybe ...)` signature from EP-2.

Edit `README.md`. Replace the existing Consumer snippet under "Usage"
with:

    ```haskell
    import Kafka.Effectful

    example :: (IOE :> es, Error KafkaError :> es) => Eff es ()
    example =
      runKafkaConsumer consumerProps subscription loop
      where
        loop = do
          mbMsg <- pollMessage (Timeout 1000)
          case mbMsg of
            Nothing  -> loop
            Just msg -> do
              commitOffsetMessage OffsetCommit msg
              loop
    ```

If EP-2 has not yet landed when this milestone is being executed, do not
make this change. Instead, record a Progress entry: "Milestone 3 blocked on
EP-2; revisit when EP-2 is complete." When EP-2 lands, un-block and apply
the edit.

Cross-check after all three milestones: read the README end-to-end and
confirm every identifier mentioned (function names, type names, import
module names) appears in the export list of `src/Kafka/Effectful.hs` or
the scoped facades `src/Kafka/Effectful/Producer.hs` and
`src/Kafka/Effectful/Consumer.hs`. If any identifier is referenced but not
exported, either add the re-export (preferably in coordination with EP-4)
or adjust the README wording to use an exported identifier.


## Concrete Steps

Run from the repository root
(`/Users/shinzui/Keikaku/bokuno/kafka-effectful`):

    git diff README.md

After each milestone, confirm the diff is what you expected.

Optional sanity check (requires a running Kafka broker, which is out of
scope for this plan): paste the `main` example from Milestone 2 into a
temporary executable stanza and run `cabal build`. If the code type-checks,
the example is correct.


## Validation and Acceptance

The plan is complete when:

1. `README.md` contains an experimental-status paragraph within the first
   40 lines.
2. `README.md` contains a complete `main` wiring example under a
   "Running it" heading that uses `runEff`, `runError`, and the Producer
   effect.
3. If EP-2 has landed, the Consumer example uses the `Maybe`-returning
   `pollMessage`.
4. Every function or type name mentioned in the README is exported by
   `Kafka.Effectful`, `Kafka.Effectful.Producer`, or
   `Kafka.Effectful.Consumer`.
5. The README renders correctly in a standard Markdown viewer (verify by
   opening in GitHub's preview, or equivalent; no local rendering step is
   strictly required).


## Idempotence and Recovery

All edits are text-only edits to `README.md`. Revert with `git restore
README.md` and re-apply. No build artifacts are involved.


## Interfaces and Dependencies

No code changes. The documentation must stay consistent with the
public API established by EP-2, EP-3, and EP-4.

Dependencies used:

- `Effectful` module — `runEff`, `IOE`, `Eff`.
- `Effectful.Error.Static` module — `runError`, `Error`.
- `Kafka.Effectful` — the library's combined facade.
