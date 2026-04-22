module Kafka.Effectful.Consumer.Effect (
    -- * Effect
    KafkaConsumer (..),

    -- * Polling
    pollMessage,
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
import Kafka.Consumer.Types (
    ConsumerRecord,
    OffsetCommit,
    SubscribedPartitions,
    TopicPartition,
 )
import Kafka.Consumer.Types qualified as KC
import Kafka.Types (
    BatchSize,
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

{- | Poll for a single message.

Returns 'Nothing' when the timeout elapses without a message arriving.
Throws 'KafkaError' via the 'Error' effect for any non-timeout failure
(for example, a broker transport error or an assignment revocation).
-}
pollMessage ::
    (KafkaConsumer :> es) =>
    Timeout ->
    Eff es (Maybe (ConsumerRecord (Maybe ByteString) (Maybe ByteString)))
pollMessage = send . PollMessage

-- | Poll for a batch of messages. Per-message errors are preserved in the 'Either'.
pollMessageBatch ::
    (KafkaConsumer :> es) =>
    Timeout ->
    BatchSize ->
    Eff es [Either KafkaError (ConsumerRecord (Maybe ByteString) (Maybe ByteString))]
pollMessageBatch t b = send $ PollMessageBatch t b

-- Offset Management

-- | Commit the offset of a specific message. Throws 'KafkaError' on failure.
commitOffsetMessage ::
    (KafkaConsumer :> es) => OffsetCommit -> ConsumerRecord k v -> Eff es ()
commitOffsetMessage oc cr = send $ CommitOffsetMessage oc cr

-- | Commit offsets for all currently assigned partitions. Throws 'KafkaError' on failure.
commitAllOffsets :: (KafkaConsumer :> es) => OffsetCommit -> Eff es ()
commitAllOffsets = send . CommitAllOffsets

-- | Commit offsets for specific partitions. Throws 'KafkaError' on failure.
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

{- | Escape hatch: return the raw @Kafka.Consumer.KafkaConsumer@ handle
acquired by @runKafkaConsumer@.

Exposed to enable the cross-effect
'Kafka.Effectful.Producer.Transaction.commitOffsetMessageTransaction'
helper, which must reach both the producer and consumer handles to
call the underlying transactional offset-commit primitive. New
operations should go through the 'KafkaConsumer' effect rather than
this handle.

@since 0.2.0.0
-}
askConsumerHandle :: (KafkaConsumer :> es) => Eff es KC.KafkaConsumer
askConsumerHandle = send AskConsumerHandle
