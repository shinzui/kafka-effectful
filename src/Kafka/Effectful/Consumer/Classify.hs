{- | How the consumer interpreters treat the errors librdkafka hands back
/in-band/ — that is, as a @Left@ inside a poll result or a @Just@ from a
commit — rather than by throwing.

Both the plain and the traced interpreter route every such error through this
module, so the policy is stated once, is unit-testable without a broker, and
cannot drift between the two interpreters.

The distinction that matters is between conditions that mean __this consumer
is broken__ and conditions that are just __flow control__. librdkafka reports
both the same way, and treating the second kind as failure is how a healthy
consumer ends up in a crash loop: the interpreter throws, the bracket closes
the consumer, the supervisor restarts it, it re-subscribes, and it meets the
same partition-scoped condition again.

@since 0.4.0.0
-}
module Kafka.Effectful.Consumer.Classify (
    -- * Poll errors
    PollErrorDisposition (..),
    classifyPollError,

    -- * Commit errors
    isBenignCommitError,
)
where

import Kafka.Consumer (RdKafkaRespErrT (..))
import Kafka.Types (KafkaError (..))

{- | What an interpreter does with an in-band poll error.

@since 0.4.0.0
-}
data PollErrorDisposition
    = {- | @RdKafkaRespErrTimedOut@: the poll interval elapsed with no
      message. Not an error at all; the poll returns 'Nothing'.
      -}
      PollTimeout
    | {- | A partition-scoped flow-control condition that a healthy consumer
      is expected to meet during normal operation. The poll returns
      'Nothing' and the consumer keeps running.
      -}
      PollBenign
    | {- | Anything else, including every fatal error: rethrown through the
      @Error@ effect.
      -}
      PollThrow
    deriving stock (Eq, Show)

{- | Classify an in-band poll error.

Three codes are treated as benign, each because librdkafka uses it to report
a normal, partition-scoped fact rather than a failure of the consumer:

* @RdKafkaRespErrPartitionEof@ — the consumer has caught up with a
  partition. Delivered on every catch-up when @enable.partition.eof@ is set,
  so throwing on it kills a consumer precisely when it has succeeded.

* @RdKafkaRespErrAutoOffsetReset@ — the consumer's position was reset, or a
  reset was attempted and refused. librdkafka raises this as a consumer error
  only under @auto.offset.reset=error@ or when a reset itself fails; a
  successful reset after retention loss merely logs. Either way the condition
  is scoped to one partition's position, and it is delivered before any new
  commit can move that position, so throwing on it loops forever.

* @RdKafkaRespErrUnknownTopicOrPart@ — the topic or partition is not known
  yet. Normal inside a topic-creation window, and it resolves itself once
  metadata propagates.

Everything else throws. That deliberately includes @RdKafkaRespErrFatal@, the
generic code librdkafka delivers once a fatal error has been raised on the
client — a fenced static group member being the canonical case — after which
the consumer is permanently dead and must be closed. It also includes
@RdKafkaRespErrSaslAuthenticationFailed@, where retrying in a poll loop never
helps and can lock accounts. Both of those are matched by the catch-all
rather than by name, so a fatal cause this table has never heard of still
throws; that is the intended failure direction.

This table is the interpreter-side counterpart of @hw-kafka-streamly@'s
@Kafka.Streamly.Stream.isFatal@, which classifies the same codes for stream
consumers. They should be read together when either changes.

@since 0.4.0.0
-}
classifyPollError :: KafkaError -> PollErrorDisposition
classifyPollError = \case
    KafkaResponseError RdKafkaRespErrTimedOut -> PollTimeout
    KafkaResponseError RdKafkaRespErrPartitionEof -> PollBenign
    KafkaResponseError RdKafkaRespErrAutoOffsetReset -> PollBenign
    KafkaResponseError RdKafkaRespErrUnknownTopicOrPart -> PollBenign
    _ -> PollThrow
{-# INLINE classifyPollError #-}

{- | Whether a commit result means "there was nothing to commit" rather than
"the commit failed".

This holds for exactly @RdKafkaRespErrNoOffset@. hw-kafka-client's own
offset-commit callback documentation says the code "is not to be considered
an error": it is what librdkafka returns when a commit is requested and no
offsets have advanced — an idle consumer, or a shutdown commit on a consumer
that never received anything.

@since 0.4.0.0
-}
isBenignCommitError :: KafkaError -> Bool
isBenignCommitError = (==) (KafkaResponseError RdKafkaRespErrNoOffset)
{-# INLINE isBenignCommitError #-}
