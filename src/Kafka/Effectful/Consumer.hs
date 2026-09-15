module Kafka.Effectful.Consumer
  ( -- * Effect
    KafkaConsumer,

    -- * Interpreter
    runKafkaConsumer,

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

    -- * Raw handle escape hatch
    askConsumerHandle,

    -- * Consumer Types
    ConsumerRecord (..),
    Offset (..),
    OffsetReset (..),
    OffsetCommit (..),
    TopicPartition (..),
    SubscribedPartitions (..),
    ConsumerGroupId (..),
    PartitionOffset (..),
    Timestamp (..),
    RebalanceEvent (..),

    -- * Configuration
    ConsumerProperties (..),
    CallbackPollMode (..),
    K.brokersList,
    K.autoCommit,
    K.noAutoCommit,
    K.noAutoOffsetStore,
    K.groupId,
    K.clientId,
    K.setCallback,
    K.logLevel,
    K.compression,
    K.suppressDisconnectLogs,
    K.statisticsInterval,
    K.extraProps,
    K.extraProp,
    K.debugOptions,
    K.queuedMaxMessagesKBytes,
    K.callbackPollMode,

    -- * Subscription
    Subscription (..),
    topics,
    offsetReset,
    extraSubscriptionProps,

    -- * Callbacks
    K.rebalanceCallback,
    K.offsetCommitCallback,
    K.errorCallback,
    K.logCallback,
    K.statsCallback,
    K.Callback,

    -- * Common Types
    KafkaError (..),
    TopicName (..),
    BrokerAddress (..),
    Timeout (..),
    BatchSize (..),
    PartitionId (..),
    Millis (..),
    ClientId (..),
    KafkaLogLevel (..),
    KafkaDebug (..),
    KafkaCompressionCodec (..),
    Headers,
    headersFromList,
    headersToList,
  )
where

import Kafka.Consumer.ConsumerProperties (CallbackPollMode (..), ConsumerProperties (..))
import Kafka.Consumer.ConsumerProperties qualified as K
import Kafka.Consumer.Subscription (Subscription (..), extraSubscriptionProps, offsetReset, topics)
import Kafka.Consumer.Types (ConsumerGroupId (..), ConsumerRecord (..), Offset (..), OffsetCommit (..), OffsetReset (..), PartitionOffset (..), RebalanceEvent (..), SubscribedPartitions (..), Timestamp (..), TopicPartition (..))
import Kafka.Effectful.Consumer.Effect (KafkaConsumer, askConsumerHandle, assign, assignment, commitAllOffsets, commitOffsetMessage, commitPartitionsOffsets, committed, pausePartitions, pollMessage, pollMessageBatch, pollMessageEither, position, resumePartitions, seekPartitions, storeOffsetMessage, storeOffsets, subscription)
import Kafka.Effectful.Consumer.Interpreter (runKafkaConsumer)
import Kafka.Types (BatchSize (..), BrokerAddress (..), ClientId (..), Headers, KafkaCompressionCodec (..), KafkaDebug (..), KafkaError (..), KafkaLogLevel (..), Millis (..), PartitionId (..), Timeout (..), TopicName (..), headersFromList, headersToList)
