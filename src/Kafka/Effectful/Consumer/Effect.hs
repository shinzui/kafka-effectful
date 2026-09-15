module Kafka.Effectful.Consumer.Effect
  ( -- * Effect
    KafkaConsumer (..),

    -- * Polling
    pollMessage,
    pollMessageEither,
    pollMessageBatch,

    -- * Offset Management
    commitOffsetMessage,
    commitAllOffsets,
    commitPartitionsOffsets,
    storeOffsets,
    storeOffsetMessage,

    -- * Partition Management
    assign,
    pausePartitions,
    resumePartitions,
    seekPartitions,

    -- * Querying
    committed,
    position,
    assignment,
    subscription,

    -- * Internal — cross-effect plumbing
    askConsumerHandle,
  )
where

import Data.ByteString (ByteString)
import Data.Map.Strict (Map)
import Effectful (Dispatch (..), DispatchOf, Eff, Effect, (:>))
import Effectful.Dispatch.Dynamic (send)
import Kafka.Consumer.Types
  ( ConsumerRecord,
    OffsetCommit,
    SubscribedPartitions,
    TopicPartition,
  )
import Kafka.Consumer.Types qualified as KC
import Kafka.Types
  ( BatchSize,
    KafkaError,
    PartitionId,
    Timeout,
    TopicName,
  )

-- | Effect for Kafka consumer operations.
data KafkaConsumer :: Effect where
  PollMessage ::
    Timeout ->
    KafkaConsumer m (Maybe (ConsumerRecord (Maybe ByteString) (Maybe ByteString)))
  PollMessageEither ::
    Timeout ->
    KafkaConsumer m (Either KafkaError (ConsumerRecord (Maybe ByteString) (Maybe ByteString)))
  PollMessageBatch ::
    Timeout ->
    BatchSize ->
    KafkaConsumer m [Either KafkaError (ConsumerRecord (Maybe ByteString) (Maybe ByteString))]
  CommitOffsetMessage ::
    OffsetCommit ->
    ConsumerRecord k v ->
    KafkaConsumer m ()
  CommitAllOffsets ::
    OffsetCommit ->
    KafkaConsumer m ()
  CommitPartitionsOffsets ::
    OffsetCommit ->
    [TopicPartition] ->
    KafkaConsumer m ()
  StoreOffsets ::
    [TopicPartition] ->
    KafkaConsumer m ()
  StoreOffsetMessage ::
    ConsumerRecord k v ->
    KafkaConsumer m ()
  Assign ::
    [TopicPartition] ->
    KafkaConsumer m ()
  PausePartitions ::
    [(TopicName, PartitionId)] ->
    KafkaConsumer m ()
  ResumePartitions ::
    [(TopicName, PartitionId)] ->
    KafkaConsumer m ()
  SeekPartitions ::
    [TopicPartition] ->
    Timeout ->
    KafkaConsumer m ()
  Committed ::
    Timeout ->
    [(TopicName, PartitionId)] ->
    KafkaConsumer m [TopicPartition]
  Position ::
    [(TopicName, PartitionId)] ->
    KafkaConsumer m [TopicPartition]
  Assignment ::
    KafkaConsumer m (Map TopicName [PartitionId])
  Subscription ::
    KafkaConsumer m [(TopicName, SubscribedPartitions)]
  AskConsumerHandle ::
    KafkaConsumer m KC.KafkaConsumer

type instance DispatchOf KafkaConsumer = 'Dynamic

-- Polling

-- | Poll for a single message.
--
-- Returns 'Nothing' when nothing was delivered, which covers the timeout and
-- three partition-scoped conditions that librdkafka reports as errors but which
-- a healthy consumer is expected to meet during normal operation:
--
-- * @RdKafkaRespErrPartitionEof@ — caught up with a partition.
-- * @RdKafkaRespErrAutoOffsetReset@ — the position was reset, or a reset was
--   refused. Raised as a consumer error only under @auto.offset.reset=error@ or
--   when a reset itself fails; a successful reset after retention loss only
--   logs.
-- * @RdKafkaRespErrUnknownTopicOrPart@ — the topic or partition is not known
--   yet, normal inside a topic-creation window.
--
-- Every other in-band error is thrown as 'KafkaError' via the 'Error' effect —
-- transport failures, authentication failures, and fatal errors among them.
--
-- Use 'pollMessageEither' when you need to observe the swallowed conditions,
-- for example to detect partition EOF in a bounded read. The full policy lives
-- in "Kafka.Effectful.Consumer.Classify".
pollMessage ::
  (KafkaConsumer :> es) =>
  Timeout ->
  Eff es (Maybe (ConsumerRecord (Maybe ByteString) (Maybe ByteString)))
pollMessage = send . PollMessage

-- | Poll for a single message, returning every in-band condition as a
-- 'Left' instead of swallowing or throwing it.
--
-- Nothing is hidden: timeouts, partition EOF, offset resets and hard failures
-- all arrive as @Left@. Use this for bounded reads that must observe partition
-- EOF to know when to stop, or wherever the full librdkafka taxonomy matters.
--
-- @since 0.4.0.0
pollMessageEither ::
  (KafkaConsumer :> es) =>
  Timeout ->
  Eff es (Either KafkaError (ConsumerRecord (Maybe ByteString) (Maybe ByteString)))
pollMessageEither = send . PollMessageEither

-- | Poll for a batch of messages. Per-message errors are preserved in the 'Either'.
pollMessageBatch ::
  (KafkaConsumer :> es) =>
  Timeout ->
  BatchSize ->
  Eff es [Either KafkaError (ConsumerRecord (Maybe ByteString) (Maybe ByteString))]
pollMessageBatch t b = send $ PollMessageBatch t b

-- Offset Management

-- | Commit the offset of a specific message. Throws 'KafkaError' on failure.
--
-- A commit that finds nothing to commit (@RdKafkaRespErrNoOffset@) is a
-- success, not a failure — see "Kafka.Effectful.Consumer.Classify".
commitOffsetMessage ::
  (KafkaConsumer :> es) => OffsetCommit -> ConsumerRecord k v -> Eff es ()
commitOffsetMessage oc cr = send $ CommitOffsetMessage oc cr

-- | Commit offsets for all currently assigned partitions. Throws
-- 'KafkaError' on failure.
--
-- An idle consumer with nothing to commit succeeds rather than throwing; see
-- 'commitOffsetMessage'.
commitAllOffsets :: (KafkaConsumer :> es) => OffsetCommit -> Eff es ()
commitAllOffsets = send . CommitAllOffsets

-- | Commit offsets for specific partitions. Throws 'KafkaError' on failure.
--
-- A commit with nothing to commit succeeds; see 'commitOffsetMessage'.
commitPartitionsOffsets ::
  (KafkaConsumer :> es) => OffsetCommit -> [TopicPartition] -> Eff es ()
commitPartitionsOffsets oc tps = send $ CommitPartitionsOffsets oc tps

-- | Store offsets locally without committing to the broker. Throws 'KafkaError' on failure.
storeOffsets :: (KafkaConsumer :> es) => [TopicPartition] -> Eff es ()
storeOffsets = send . StoreOffsets

-- | Store a message's offset locally without committing. Throws 'KafkaError' on failure.
storeOffsetMessage :: (KafkaConsumer :> es) => ConsumerRecord k v -> Eff es ()
storeOffsetMessage = send . StoreOffsetMessage

-- Partition Management

-- | Manually assign partitions to the consumer. Throws 'KafkaError' on failure.
assign :: (KafkaConsumer :> es) => [TopicPartition] -> Eff es ()
assign = send . Assign

-- | Pause consuming from the specified partitions. Throws 'KafkaError' on failure.
pausePartitions :: (KafkaConsumer :> es) => [(TopicName, PartitionId)] -> Eff es ()
pausePartitions = send . PausePartitions

-- | Resume consuming from the specified partitions. Throws 'KafkaError' on failure.
resumePartitions :: (KafkaConsumer :> es) => [(TopicName, PartitionId)] -> Eff es ()
resumePartitions = send . ResumePartitions

-- | Seek to specific offsets for partitions. Throws 'KafkaError' on failure.
seekPartitions :: (KafkaConsumer :> es) => [TopicPartition] -> Timeout -> Eff es ()
seekPartitions tps t = send $ SeekPartitions tps t

-- Querying

-- | Get committed offsets for the specified partitions. Throws 'KafkaError' on failure.
committed ::
  (KafkaConsumer :> es) =>
  Timeout ->
  [(TopicName, PartitionId)] ->
  Eff es [TopicPartition]
committed t ps = send $ Committed t ps

-- | Get the current position (last consumed offset + 1). Throws 'KafkaError' on failure.
position ::
  (KafkaConsumer :> es) =>
  [(TopicName, PartitionId)] ->
  Eff es [TopicPartition]
position = send . Position

-- | Get the current partition assignment.
assignment :: (KafkaConsumer :> es) => Eff es (Map TopicName [PartitionId])
assignment = send Assignment

-- | Get the current topic subscription.
subscription :: (KafkaConsumer :> es) => Eff es [(TopicName, SubscribedPartitions)]
subscription = send Subscription

-- | Escape hatch: return the raw @Kafka.Consumer.KafkaConsumer@ handle
-- acquired by @runKafkaConsumer@.
--
-- Exposed to enable the cross-effect
-- 'Kafka.Effectful.Producer.Transaction.commitOffsetMessageTransaction'
-- helper, which must reach both the producer and consumer handles to
-- call the underlying transactional offset-commit primitive. New
-- operations should go through the 'KafkaConsumer' effect rather than
-- this handle.
--
-- @since 0.2.0.0
askConsumerHandle :: (KafkaConsumer :> es) => Eff es KC.KafkaConsumer
askConsumerHandle = send AskConsumerHandle
