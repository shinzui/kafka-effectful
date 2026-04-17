# Fix consumer polling timeout semantics

MasterPlan: docs/masterplans/1-prepare-kafka-effectful-0-1-release.md
Intention: intention_01km3c2s7xeamb7gkfjkve90ma

This ExecPlan is a living document. The sections Progress, Surprises & Discoveries,
Decision Log, and Outcomes & Retrospective must be kept up to date as work proceeds.

This document is maintained in accordance with `.claude/skills/exec-plan/PLANS.md`.


## Purpose / Big Picture

Today, `pollMessage` in `Kafka.Effectful.Consumer` throws a `KafkaError` on every
timeout. `hw-kafka-client` signals a timeout by returning
`Left (KafkaResponseError RdKafkaRespErrTimedOut)`, which this library's
interpreter currently converts into `throwError err`. Since timeouts are the
normal case in a polling loop — the caller wants a `Timeout 100`, no message
yet, poll again — every caller has to wrap `pollMessage` in `catchError` and
pattern-match on the timeout variant. That is unpleasant to use and easy to get
wrong.

After this change, `pollMessage` will return
`Maybe (ConsumerRecord (Maybe ByteString) (Maybe ByteString))`. `Nothing` means
"no message arrived within the timeout"; `Just record` means a message was
received. Non-timeout errors continue to throw via `Effectful.Error.Static`.
A user writing a polling loop then writes:

    loop = do
        mbMsg <- pollMessage (Timeout 1000)
        case mbMsg of
            Nothing  -> loop
            Just msg -> processAndCommit msg

and no `catchError` is needed on the hot path.

This is a breaking change to the public API of an `0.0.x`-era library. It is
safe to make before `0.1.0.0` is tagged but must not be made after. The work
is user-visible and verifiable by reading the resulting example in the README
and by observing that a consumer example program type-checks without
`catchError`.

The batch variant `pollMessageBatch` already returns
`[Either KafkaError (ConsumerRecord ...)]`; it preserves per-message errors
inside the list and is not changed by this plan.


## Progress

- [x] Update `Kafka.Effectful.Consumer.Effect.PollMessage` GADT constructor return type to `Maybe`. (2026-04-16)
- [x] Update `pollMessage` wrapper signature and Haddock in `src/Kafka/Effectful/Consumer/Effect.hs`. (2026-04-16)
- [x] Update the `PollMessage` handler in `src/Kafka/Effectful/Consumer/Interpreter.hs` to translate `RdKafkaRespErrTimedOut` to `Nothing` and throw on all other errors. (2026-04-16)
- [x] Update the consumer example in `README.md` to use the new shape. (2026-04-16)
- [x] Run `cabal build` and confirm clean compile (modulo the known `-Wredundant-constraints` upstream warning). (2026-04-16)
- [x] Write Outcomes & Retrospective. (2026-04-16)


## Surprises & Discoveries

- `RdKafkaRespErrT (..)` is re-exported from `Kafka.Consumer` (seen at
  hw-kafka-client's `src/Kafka/Consumer.hs:66`). The first import variant
  suggested in the plan worked; no fallback to `Kafka.Types` or
  `Kafka.Internal.RdKafka` was needed.


## Decision Log

- Decision: Return `Maybe` rather than `Either KafkaError`.
  Rationale: `Either` forces every caller to branch on two error paths (timeout
  and real error) even though timeout is the only expected "not-an-error"
  outcome. `Maybe` aligns with common polling APIs (e.g. STM `tryReadTChan`,
  `Network.HTTP.Client.withResponse` timeouts lifted to `Maybe`). Real
  non-timeout errors still throw via the `Error KafkaError` effect, keeping the
  happy path flat.
  Date: 2026-04-16

- Decision: Do not change `pollMessageBatch`.
  Rationale: The batch variant in hw-kafka-client returns an empty list on
  timeout rather than an error, and preserves per-message errors in the list
  via `Either`. The existing type already communicates this correctly.
  Date: 2026-04-16

- Decision: Reconsidered `Either` vs `Maybe` and kept `Maybe`.
  Rationale: Briefly considered returning `Either KafkaError (ConsumerRecord
  ...)` (mirroring hw-kafka-client's raw shape) or `Either PollTimeout
  (ConsumerRecord ...)` (named timeout tag). The raw-`Either` form would
  remove throwing entirely but reintroduces the two-error-paths problem the
  original decision rejected. The named-tag `Either` is isomorphic to `Maybe`
  with no added information, since timeout is the only `Left` value. `Maybe`
  remains the simplest faithful encoding.
  Date: 2026-04-16


## Outcomes & Retrospective

`pollMessage` now returns `Maybe (ConsumerRecord (Maybe ByteString) (Maybe
ByteString))`. Timeouts (`KafkaResponseError RdKafkaRespErrTimedOut`) map to
`Nothing`; all other `Left` values continue to throw via `Error KafkaError`.
The consumer example in the README was updated to show the idiomatic polling
loop using `case mbMsg of Nothing -> loop; Just msg -> ...`. `cabal build`
compiles cleanly with only the pre-existing `-Wredundant-constraints`
warnings that EP-3 will address.

The only minor deviation from the plan was the need to choose between three
possible import paths for `RdKafkaRespErrT (..)`; the first suggested path
(`Kafka.Consumer`) worked without further investigation.

If EP-6 (docs polish) runs after this plan, no follow-up edit to the README
example is required.


## Context and Orientation

`kafka-effectful` is a thin wrapper library. The consumer effect lives in two
files:

- `src/Kafka/Effectful/Consumer/Effect.hs` defines `KafkaConsumer :: Effect`
  as a GADT. Each constructor maps one-to-one to an operation. The
  `PollMessage` constructor is currently declared:

        PollMessage ::
            Timeout ->
            KafkaConsumer m (ConsumerRecord (Maybe ByteString) (Maybe ByteString))

  and the wrapper is:

        pollMessage ::
            (KafkaConsumer :> es) =>
            Timeout ->
            Eff es (ConsumerRecord (Maybe ByteString) (Maybe ByteString))
        pollMessage = send . PollMessage

- `src/Kafka/Effectful/Consumer/Interpreter.hs` contains the
  `runKafkaConsumer` interpreter and the `handleConsumer` dispatch function.
  The current `PollMessage` branch reads:

        PollMessage timeout -> do
            result <- Effectful.liftIO $ K.pollMessage consumer timeout
            case result of
                Left err -> throwError err
                Right msg -> pure msg

Relevant hw-kafka-client context (read from
`/Users/shinzui/Keikaku/hub/haskell/hw-kafka-client-project/hw-kafka-client/src/Kafka/Consumer.hs`
during research):

- `K.pollMessage :: MonadIO m => KafkaConsumer -> Timeout -> m (Either KafkaError (ConsumerRecord (Maybe ByteString) (Maybe ByteString)))`.
- A timeout is surfaced as `Left (KafkaResponseError RdKafkaRespErrTimedOut)`.
- `KafkaError` is defined in `Kafka.Types`; its `KafkaResponseError` constructor
  wraps `RdKafkaRespErrT`, an enum type whose `RdKafkaRespErrTimedOut` variant
  corresponds to librdkafka's `RD_KAFKA_RESP_ERR__TIMED_OUT`.

The library already imports `Kafka.Types (KafkaError (..))` in the interpreter
file, which gives it access to `KafkaResponseError` as a pattern. The
`RdKafkaRespErrTimedOut` constructor is re-exported from `Kafka.Types` via
the hw-kafka-client public module structure — if it is not directly visible,
it can be imported from `Kafka.Consumer.Types` or `Kafka.Internal.RdKafka` as
necessary. Expect it to be reachable via the existing `Kafka.Consumer`
re-exports since the interpreter already uses `K.pollMessage`.

The README contains a short consumer example whose current form is:

    example =
      runKafkaConsumer consumerProps subscription $ do
        msg <- pollMessage (Timeout 1000)
        commitOffsetMessage OffsetCommit msg

This example is broken under the new shape because `pollMessage` now returns
`Maybe`. It must be updated to the `case mbMsg of ...` form shown in the
Purpose section above.

The combined facade at `src/Kafka/Effectful.hs` re-exports `pollMessage`. The
scoped facade at `src/Kafka/Effectful/Consumer.hs` also re-exports it. Neither
file needs type changes (re-exports carry whatever the original type is), but
their Haddock export lists may contain a section comment referring to poll
behavior that should be spot-checked.

The MasterPlan at `docs/masterplans/1-prepare-kafka-effectful-0-1-release.md`
notes that this plan is the only source of the breaking API change in the
release, and that EP-6 (docs polish) soft-depends on this plan. If EP-6 runs
before this plan, its README example will need a follow-up edit; if after, no
follow-up is needed.


## Plan of Work

There is only one milestone.

**Milestone 1: Return `Maybe` from `pollMessage`.**

Scope: modify the effect GADT, the wrapper function signature, the interpreter
handler, and the README example. After the milestone, `cabal build` succeeds and
a user can write a polling loop without `catchError`.

Work:

1. Edit `src/Kafka/Effectful/Consumer/Effect.hs`.

   Change the `PollMessage` constructor declaration:

        PollMessage ::
            Timeout ->
            KafkaConsumer m (Maybe (ConsumerRecord (Maybe ByteString) (Maybe ByteString)))

   Update the `pollMessage` wrapper:

        pollMessage ::
            (KafkaConsumer :> es) =>
            Timeout ->
            Eff es (Maybe (ConsumerRecord (Maybe ByteString) (Maybe ByteString)))
        pollMessage = send . PollMessage

   Update the Haddock on `pollMessage`. Replace the current comment:

        -- | Poll for a single message. Throws 'KafkaError' on failure (including timeout).

   with:

        -- | Poll for a single message.
        --
        -- Returns 'Nothing' when the timeout elapses without a message arriving.
        -- Throws 'KafkaError' via the 'Error' effect for any non-timeout failure
        -- (for example, a broker transport error or an assignment revocation).

2. Edit `src/Kafka/Effectful/Consumer/Interpreter.hs`.

   Replace the `PollMessage timeout -> ...` branch of the `handleConsumer`
   case expression. The new branch must:

   - Call `K.pollMessage consumer timeout` inside `Effectful.liftIO`.
   - On `Left (KafkaResponseError RdKafkaRespErrTimedOut)`, return `Nothing`.
   - On any other `Left err`, `throwError err`.
   - On `Right msg`, return `Just msg`.

   Concretely:

        PollMessage timeout -> do
            result <- Effectful.liftIO $ K.pollMessage consumer timeout
            case result of
                Left (KafkaResponseError RdKafkaRespErrTimedOut) -> pure Nothing
                Left err -> throwError err
                Right msg -> pure (Just msg)

   The pattern `RdKafkaRespErrTimedOut` may require an additional import. If
   GHC reports it is not in scope after the edit, add to the imports block:

        import Kafka.Consumer (RdKafkaRespErrT (..))

   or, if not re-exported there, try:

        import Kafka.Types (RdKafkaRespErrT (..))

   or, as a last resort, import from the internal module:

        import Kafka.Internal.RdKafka (RdKafkaRespErrT (..))

   Whichever import actually works, keep it grouped with the existing
   `Kafka.*` imports at the top of the file.

3. Edit `README.md`.

   Locate the `### Consumer` section. Replace the example block:

        example =
          runKafkaConsumer consumerProps subscription $ do
            msg <- pollMessage (Timeout 1000)
            commitOffsetMessage OffsetCommit msg

   with:

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

   The outer type signature on `example` can remain unchanged.

4. Check the scoped facade Haddocks. Open
   `src/Kafka/Effectful/Consumer.hs` and confirm that the `-- * Polling`
   section comment does not make claims about timeout behavior. If it
   mentions timeout handling, adjust to match the new semantics or remove
   the claim. The combined facade `src/Kafka/Effectful.hs` uses the same
   section layout and should be checked for the same reason. No code changes
   are expected here — only comment text if present.

5. Build the library.

        cabal build

   Expect only the pre-existing `-Wredundant-constraints` warnings on
   `handleConsumer` and `handleProducer` to appear. Any new errors or
   warnings attributable to this change must be resolved before marking
   the milestone complete.


## Concrete Steps

Run the following commands from the repository root
(`/Users/shinzui/Keikaku/bokuno/kafka-effectful`):

    cabal build

Expected transcript excerpt on success:

    Build profile: -w ghc-9.12.2 -O1
    In order, the following will be built (use -v for more details):
     - kafka-effectful-0.1.0.0 (lib) (configuration changed)
    Configuring library for kafka-effectful-0.1.0.0...
    Preprocessing library for kafka-effectful-0.1.0.0...
    Building library for kafka-effectful-0.1.0.0...
    [1 of 7] Compiling Kafka.Effectful.Consumer.Effect
    [2 of 7] Compiling Kafka.Effectful.Consumer.Interpreter

The two known `-Wredundant-constraints` warnings on `handleConsumer` and
`handleProducer` will still fire until EP-3 addresses them. That is expected.


## Validation and Acceptance

The change is effective when all of the following are true:

1. `cabal build` succeeds with no errors.
2. The only warnings are the two upstream `-Wredundant-constraints` warnings
   on `EffectHandler KafkaConsumer es` and `EffectHandler KafkaProducer es`.
3. The type signature reported by `cabal repl` for `pollMessage` is:

        *Kafka.Effectful.Consumer> :t pollMessage
        pollMessage
          :: (KafkaConsumer :> es) =>
             Timeout
             -> Eff es (Maybe (ConsumerRecord (Maybe ByteString) (Maybe ByteString)))

4. The README consumer example reads as shown in step 3 of the Plan of Work
   and contains no `catchError` call on the polling line.

Optional manual check: open a GHCi session with
`cabal repl kafka-effectful` and load a small expression:

    :set -XTypeApplications
    :t \mbMsg -> case mbMsg of { Nothing -> pure (); Just _ -> pure () }

This is not a substitute for real broker testing — no live broker is assumed
in this plan — but it confirms the shape.


## Idempotence and Recovery

Every step is a simple textual edit. If a step fails, revert the file with
`git restore <path>` and re-apply. The imports adjustment in step 2 may need
one of three variants depending on hw-kafka-client's module layout; if GHC
reports a different module should be used, follow the GHC suggestion and
record the module name in Surprises & Discoveries.


## Interfaces and Dependencies

After this plan, the public interface of `Kafka.Effectful.Consumer` and
`Kafka.Effectful` includes:

    pollMessage ::
        (KafkaConsumer :> es) =>
        Timeout ->
        Eff es (Maybe (ConsumerRecord (Maybe ByteString) (Maybe ByteString)))

All other exports remain as documented in
`docs/plans/1-effectful-bindings-for-hw-kafka-client.md`.

Dependencies used:

- `hw-kafka-client` >= 5.3 && < 6 (`K.pollMessage` and the
  `RdKafkaRespErrTimedOut` enum variant).
- `effectful-core` ^>= 2.5 || ^>= 2.6 (`Eff`, `send`, `interpret`,
  `Error`, `throwError`).
- `bytestring` (`ByteString` in the record payload type).
