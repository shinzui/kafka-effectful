# Harden producer and consumer interpreters

MasterPlan: docs/masterplans/1-prepare-kafka-effectful-0-1-release.md
Intention: intention_01km3c2s7xeamb7gkfjkve90ma

This ExecPlan is a living document. The sections Progress, Surprises & Discoveries,
Decision Log, and Outcomes & Retrospective must be kept up to date as work proceeds.

This document is maintained in accordance with `.claude/skills/exec-plan/PLANS.md`.


## Purpose / Big Picture

Four internal defects in the two interpreter modules (`Producer/Interpreter.hs`
and `Consumer/Interpreter.hs`) and one cabal-level warning need to be addressed
before 0.1. None are user-visible at the API type level, but each one is either
a resource-safety issue, a brittle idiom, or a warning that greets anyone who
builds the library. After this plan:

- The producer and consumer handles cannot leak on asynchronous exceptions
  during the gap between `newProducer` / `newConsumer` returning and `bracket`
  installing a cleanup handler.
- The `pausePartitions` and `resumePartitions` handlers pattern-match
  directly on `RdKafkaRespErrNoError` (the "no error" variant of the
  librdkafka response enum) instead of comparing via `toEnum 0`, which is a
  brittle encoding.
- A failure returned by `K.closeConsumer` during bracket cleanup does not
  mask an exception thrown from the user action — the original failure is
  preserved and the close error is surfaced without clobbering it.
- The `-Wredundant-constraints` warnings fired by the `EffectHandler`
  type synonym on `handleProducer` and `handleConsumer` are suppressed
  locally so that `cabal build` is silent for a clean checkout.

How to see it working: `cabal build` finishes with zero warnings (the two
previously-known `-Wredundant-constraints` warnings on the interpreter
handlers are gone), and a code reader of both `Interpreter.hs` files sees
direct pattern matches on `RdKafkaRespErrNoError` rather than a
`rdErr == toEnum 0` comparison.


## Progress

- [x] Restructure `runKafkaProducer` in `src/Kafka/Effectful/Producer/Interpreter.hs` so that `newProducer` lives inside `Exception.bracket`'s acquisition action. (2026-04-16)
- [x] Restructure `runKafkaConsumer` in `src/Kafka/Effectful/Consumer/Interpreter.hs` to acquire inside `bracket`. (2026-04-16)
- [x] Adjust consumer close-error handling via `Effectful.Exception.generalBracket` so close failures do not mask user-action exceptions. (2026-04-16)
- [x] Add a `throwOnKafkaErr` helper in `Consumer/Interpreter.hs` and use it from the `PausePartitions` and `ResumePartitions` branches; remove the `toEnum 0` idiom. (2026-04-16)
- [x] Suppress the `-Wredundant-constraints` warning for both interpreter modules via per-file `{-# OPTIONS_GHC -Wno-redundant-constraints #-}` pragma. (2026-04-16)
- [x] Run `cabal clean && cabal build` and confirm it completes with zero warnings. (2026-04-16)
- [x] Write Outcomes & Retrospective. (2026-04-16)


## Surprises & Discoveries

- `Effectful.Exception` (effectful-core 2.6) re-exports both
  `generalBracket` and `ExitCase(..)` directly (sourced from
  `Control.Monad.Catch`), so no extra dependency or import from
  `UnliftIO.Exception` was needed. The `release` callback is invoked
  with `ExitCaseSuccess`, `ExitCaseException`, or `ExitCaseAbort`,
  matching the plan's expected shape exactly. (2026-04-16)


## Decision Log

- Decision: Use a per-file `{-# OPTIONS_GHC -Wno-redundant-constraints #-}`
  pragma on both interpreter modules rather than a cabal-level scoped stanza.
  Rationale: Local and self-documenting. A future reader of the file sees
  immediately why the otherwise-undeclared constraints do not trip the
  warning. Adding a scoped cabal stanza would either require a second
  `library` stanza or `ghc-options:` per-module which is awkward.
  Date: 2026-04-16

- Decision: During bracket cleanup, log-or-swallow is unavailable (there is
  no logger in scope), so close errors are surfaced via
  `Effectful.Exception.throwIO` only if the main action completed normally.
  If the main action already threw, the close error is suppressed. This
  matches the behavior of `Control.Exception.bracket` in base: the cleanup
  action runs inside `uninterruptibleMask_` and may raise, but the original
  exception from the body has priority when both exist.
  Rationale: Preserves the user's original failure signal; the close error
  is secondary information and would otherwise silently overwrite a more
  actionable failure.
  Date: 2026-04-16


## Outcomes & Retrospective

All four milestones landed cleanly across three commits (one per
behavior change; milestone 4's pragma suppression is bundled with the
final validation).

- The producer and consumer handles are now acquired *inside*
  `Exception.bracket`'s acquisition action, eliminating the
  async-exception window between handle creation and the bracket mask.
- `runKafkaConsumer` uses `Effectful.Exception.generalBracket` so a
  failing user action inside the consumer scope surfaces the user's
  exception, while a `closeConsumer` failure on a clean exit is still
  raised through the `Error KafkaError` effect. `generalBracket` and
  `ExitCase(..)` are both re-exported by effectful-core 2.6 and
  required no new dependency.
- `PausePartitions` and `ResumePartitions` now route through a local
  `throwOnKafkaErr` helper that pattern-matches on
  `RdKafkaRespErrNoError` directly, replacing the brittle
  `rdErr == toEnum 0` comparison.
- A per-file `{-# OPTIONS_GHC -Wno-redundant-constraints #-}` pragma
  with an explanatory comment was added at the top of both interpreter
  modules. `cabal clean && cabal build` now finishes with no warning
  lines from any module in the package.

No public type or function signature changed. The plan's expected
acceptance criteria all hold:

1. `cabal clean && cabal build` prints no warnings.
2. `grep "toEnum 0" src/Kafka/Effectful` returns no matches.
3. Both interpreter files begin with the suppressing pragma and a
   short explanatory comment.
4. `K.newProducer` / `K.newConsumer` live inside the bracket
   acquisition action.
5. The consumer's release path uses `generalBracket` and only raises
   close errors when the body completed via `ExitCaseSuccess`.


## Context and Orientation

`kafka-effectful` is an Effectful wrapper around `hw-kafka-client`. The two
interpreter modules this plan touches are the only places in the library that
call into hw-kafka-client's `MonadIO` API:

- `src/Kafka/Effectful/Producer/Interpreter.hs`
- `src/Kafka/Effectful/Consumer/Interpreter.hs`

Both currently follow the pattern:

    run... props action = do
        result <- Effectful.liftIO $ K.newX props
        case result of
            Left err -> throwError err
            Right handle ->
                Exception.bracket
                    (pure handle)
                    (Effectful.liftIO . K.closeX)
                    (\h -> interpret (handleX h) action)

The problem with `bracket (pure handle) ...` is that the handle is acquired
*before* bracket enters its mask scope. If an asynchronous exception is
delivered between `K.newX` returning `Right handle` and `bracket` masking,
the handle leaks. The window is small but real, and easy to close by moving
`K.newX` inside bracket's acquisition action.

The `pausePartitions` and `resumePartitions` branches of `handleConsumer`
currently read:

    PausePartitions parts -> do
        err <- Effectful.liftIO $ K.pausePartitions consumer parts
        case err of
            KafkaResponseError rdErr
                | rdErr == toEnum 0 -> pure ()
            _ -> throwError err

The `rdErr == toEnum 0` comparison relies on `RdKafkaRespErrNoError` being
enum value zero, which is true today but brittle. hw-kafka-client's internal
module `Kafka.Internal.Shared` uses a direct pattern match via
`kafkaErrorToMaybe`:

    kafkaErrorToMaybe :: KafkaError -> Maybe KafkaError
    kafkaErrorToMaybe err = case err of
        KafkaResponseError RdKafkaRespErrNoError -> Nothing
        _                                        -> Just err

`kafkaErrorToMaybe` is in an internal module and therefore not re-usable.
This plan introduces a local helper with the same semantics specialized to
an `IO (KafkaError)` action.

The `-Wredundant-constraints` warning fires with text like:

    src/Kafka/Effectful/Consumer/Interpreter.hs:45:5: warning: [GHC-30606] [-Wredundant-constraints]
        Redundant constraint: KafkaConsumer :> localEs
        In the type signature for:
             handleConsumer :: K.KafkaConsumer -> EffectHandler KafkaConsumer es

This is caused by the shape of `EffectHandler`'s type synonym expansion in
effectful-core 2.5/2.6. It is not actionable at the call site because the
constraint is genuinely required for `interpret` to type-check, and declaring
it fully makes the compiler (incorrectly) flag part of it as redundant.
Suppressing the warning in the two affected files is the pragmatic fix. The
cabal-wide `ghc-options` block already enables a long list of warnings
including `-Wredundant-constraints` (transitively via `-Wall`).

Relevant imports already in use:

- `Effectful.Exception qualified as Exception` — used for `bracket`. This
  module wraps `Control.Exception.bracket` to interact correctly with
  `Effectful.Error.Static`.
- `Effectful.Error.Static (Error, throwError)` — effectful errors surface
  here.
- `Kafka.Types (KafkaError (..))` in the consumer interpreter — provides
  the `KafkaResponseError` constructor.
- `Kafka.Consumer qualified as K` and `Kafka.Producer qualified as K` — the
  hw-kafka-client public API.

`RdKafkaRespErrNoError` is the no-error variant of `RdKafkaRespErrT`. It is
exposed through hw-kafka-client. If not already in scope in the consumer
interpreter module, it must be imported from `Kafka.Consumer` (try first)
or `Kafka.Types`.

This plan does not change any public type or rename any function. All edits
are internal to the interpreter modules and (for the warning suppression) the
file header.

The MasterPlan at `docs/masterplans/1-prepare-kafka-effectful-0-1-release.md`
records the relationship with other in-flight plans. Notably, EP-2 also edits
the `PollMessage` branch of `handleConsumer`. The edits in this plan (adding
a `throwOnKafkaErr` helper, restructuring `bracket`, and tweaking the close
handler) do not touch the `PollMessage` branch, so the two plans can land in
either order. If EP-2 has already landed, `PollMessage` will already return
`Maybe`; leave it alone.


## Plan of Work

Four small milestones, each independently verifiable.

**Milestone 1: Close the acquisition window in both interpreters.**

Scope: `runKafkaProducer` and `runKafkaConsumer` acquire their handles
through `Exception.bracket`'s acquisition action instead of outside of
bracket. After the milestone, `cabal build` succeeds.

Edits:

1. In `src/Kafka/Effectful/Producer/Interpreter.hs`, rewrite
   `runKafkaProducer`:

        runKafkaProducer ::
            (IOE :> es, Error KafkaError :> es) =>
            ProducerProperties ->
            Eff (KafkaProducer : es) a ->
            Eff es a
        runKafkaProducer props action =
            Exception.bracket
                acquire
                (Effectful.liftIO . K.closeProducer)
                (\producer -> interpret (handleProducer producer) action)
          where
            acquire = do
                result <- Effectful.liftIO $ K.newProducer props
                case result of
                    Left err -> throwError err
                    Right producer -> pure producer

   Notice that `throwError` runs inside `acquire` before any handle exists,
   so nothing needs to be cleaned up if `newProducer` returns `Left`.

2. In `src/Kafka/Effectful/Consumer/Interpreter.hs`, rewrite
   `runKafkaConsumer` using the same pattern but with the consumer's
   `Maybe KafkaError`-returning close:

        runKafkaConsumer ::
            (IOE :> es, Error KafkaError :> es) =>
            ConsumerProperties ->
            Subscription ->
            Eff (KafkaConsumer : es) a ->
            Eff es a
        runKafkaConsumer props sub action =
            Exception.bracket
                acquire
                release
                (\consumer -> interpret (handleConsumer consumer) action)
          where
            acquire = do
                result <- Effectful.liftIO $ K.newConsumer props sub
                case result of
                    Left err -> throwError err
                    Right consumer -> pure consumer

            release consumer = do
                mbErr <- Effectful.liftIO $ K.closeConsumer consumer
                for_ mbErr throwError

   The `release` function is explicit so milestone 2 can modify it.

Verification: `cabal build` succeeds with the same set of warnings as today.

**Milestone 2: Do not mask user action errors with close errors.**

Scope: the consumer's `release` function must not call `throwError` when
`bracket` is unwinding due to an exception from the user action. After the
milestone, a failing user action inside `runKafkaConsumer` surfaces the
user's error; a failing `closeConsumer` on a successful run still surfaces
the close error.

`Effectful.Exception.bracket` runs the release action in both the success
and failure paths of the body. If the body threw, its exception is
re-raised after release completes. If release also throws, the release
exception replaces the body exception in the default `Control.Exception`
semantics. To avoid that masking, use the `generalBracket` pattern from
`Effectful.Exception` or use `onException` + a manual success-only
cleanup.

Recommended implementation using `Effectful.Exception.generalBracket`:

    import Effectful.Exception (ExitCase (..), generalBracket)

    runKafkaConsumer props sub action =
        fst <$> generalBracket acquire release
                    (\consumer -> interpret (handleConsumer consumer) action)
      where
        acquire = ...
        release consumer exitCase = case exitCase of
            ExitCaseSuccess _ -> do
                mbErr <- Effectful.liftIO $ K.closeConsumer consumer
                for_ mbErr throwError
            ExitCaseException _ -> do
                _ <- Effectful.liftIO $ K.closeConsumer consumer
                pure ()
            ExitCaseAbort -> do
                _ <- Effectful.liftIO $ K.closeConsumer consumer
                pure ()

If `generalBracket` is not available directly from
`Effectful.Exception` in effectful-core 2.5/2.6, check whether it is
re-exported from `Effectful.Exception.Dynamic` or must be imported from
`UnliftIO.Exception`. The library already depends transitively on
unliftio-core through effectful; a direct import is acceptable. If no
`generalBracket` is reachable without new dependencies, use the
simpler pattern:

    runKafkaConsumer props sub action =
        Exception.bracket acquire closeQuietly $ \consumer -> do
            a <- interpret (handleConsumer consumer) action
            mbErr <- Effectful.liftIO $ K.closeConsumer consumer
            -- consumer is closed twice on the happy path — see note below
            for_ mbErr throwError
            pure a

which is not correct because double-close is not safe. The preferred
pattern is `generalBracket`. If it is genuinely unavailable, document
the limitation in Surprises & Discoveries and leave the behavior as-is
(close errors can mask body errors) — do not ship a subtly wrong fix.

The producer close (`K.closeProducer`) returns `()` and cannot fail, so no
change is needed for the producer.

Verification: write a small ad-hoc test in ghci (not committed) that
runs `runKafkaConsumer` with invalid broker settings, triggers a user-code
`throwError`, and confirms the original `throwError` surfaces rather than
a `closeConsumer` error. This is optional because live-broker testing is
out of scope; if ghci-level synthesis is impractical, mark the verification
as "reviewed by code inspection" in the Progress section.

**Milestone 3: Replace `toEnum 0` with a direct pattern match.**

Scope: add a `throwOnKafkaErr` helper in
`src/Kafka/Effectful/Consumer/Interpreter.hs` and use it from the
`PausePartitions` and `ResumePartitions` branches.

Edits:

1. Ensure `RdKafkaRespErrNoError` is in scope. If the existing import
   `import Kafka.Types (KafkaError (..))` does not bring
   `RdKafkaRespErrT (..)` into scope, add one of:

        import Kafka.Consumer (RdKafkaRespErrT (..))

   or

        import Kafka.Types (RdKafkaRespErrT (..))

   — whichever the compiler accepts. If the constructor needs a direct
   import from the internal module, use:

        import Kafka.Internal.RdKafka (RdKafkaRespErrT (..))

2. Add a helper to the `where` block of `handleConsumer`:

        throwOnKafkaErr act = do
            err <- Effectful.liftIO act
            case err of
                KafkaResponseError RdKafkaRespErrNoError -> pure ()
                _ -> throwError err

3. Replace the `PausePartitions` branch:

        PausePartitions parts ->
            throwOnKafkaErr (K.pausePartitions consumer parts)

4. Replace the `ResumePartitions` branch:

        ResumePartitions parts ->
            throwOnKafkaErr (K.resumePartitions consumer parts)

Verification: `cabal build` succeeds with no warnings attributable to this
change.

**Milestone 4: Suppress `-Wredundant-constraints` in the two interpreter
modules.**

Scope: both interpreter modules compile without warnings. The suppression is
file-local and documented.

Edits:

1. At the top of `src/Kafka/Effectful/Producer/Interpreter.hs`, on a line
   before the `module` declaration, add:

        {-# OPTIONS_GHC -Wno-redundant-constraints #-}

   with a short companion comment above it explaining why:

        -- The 'EffectHandler' type synonym in effectful-core surfaces a
        -- constraint that GHC flags as redundant even though it is required
        -- for 'interpret'. See:
        --   https://github.com/haskell-effectful/effectful/issues  (upstream)

2. Apply the same pragma and comment at the top of
   `src/Kafka/Effectful/Consumer/Interpreter.hs`.

Verification: `cabal build` output contains no `-Wredundant-constraints`
lines at all.


## Concrete Steps

Run the following commands from the repository root
(`/Users/shinzui/Keikaku/bokuno/kafka-effectful`):

    cabal build

After milestone 1, expect the build to succeed with the two pre-existing
`-Wredundant-constraints` warnings still present.

After milestone 2, the build still succeeds with the same warning set.

After milestone 3, no new warnings; the `toEnum 0` idiom is gone.

After milestone 4, the build output for the library itself contains no
warnings. Example expected tail:

    [5 of 7] Compiling Kafka.Effectful.Producer.Interpreter
    [7 of 7] Compiling Kafka.Effectful

with no warning lines between them.


## Validation and Acceptance

The plan is complete when all of the following are observed:

1. `cabal clean && cabal build` prints no warnings from any module in this
   package.
2. `grep -n "toEnum 0" src/Kafka/Effectful` returns no matches.
3. The two interpreter files each begin with
   `{-# OPTIONS_GHC -Wno-redundant-constraints #-}` and a short comment
   explaining it.
4. Both `runKafkaProducer` and `runKafkaConsumer` call `K.newX` inside the
   `bracket` acquisition action (or `generalBracket` equivalent), never
   before it.
5. Code inspection confirms that the consumer's release path suppresses
   close errors when the body raised an exception, or — if
   `generalBracket` was unavailable and the simple pattern was kept —
   that this limitation is recorded in Surprises & Discoveries with a
   pointer to a follow-up.


## Idempotence and Recovery

All edits are textual and scoped to two files plus the cabal file (no
edit to the cabal file if milestone 4 uses the per-file pragma approach).
Revert any step with `git restore <path>` and re-apply. If
`generalBracket` proves unavailable, document the gap rather than
inventing a workaround that double-closes the consumer handle —
`K.closeConsumer` is not documented to be safe under double invocation.


## Interfaces and Dependencies

No public interface changes. The exported type signatures of
`runKafkaProducer` and `runKafkaConsumer` remain exactly:

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

Dependencies used:

- `effectful-core` ^>= 2.5 || ^>= 2.6 — `Effectful.Exception.bracket` and,
  if available, `Effectful.Exception.generalBracket`.
- `hw-kafka-client` >= 5.3 && < 6 — `K.newProducer`, `K.closeProducer`,
  `K.newConsumer`, `K.closeConsumer`, `K.pausePartitions`,
  `K.resumePartitions`, and the `RdKafkaRespErrNoError` enum variant.
