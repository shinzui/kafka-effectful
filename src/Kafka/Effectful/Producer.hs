module Kafka.Effectful.Producer
  ( -- * Effect
    KafkaProducer,

    -- * Interpreter
    runKafkaProducer,

    -- * Operations
    produceMessage,
    produceMessage',
    produceMessageSync,
    produceMessageBatch,
    flushProducer,

    -- * Transactions
    initTransactions,
    beginTransaction,
    commitTransaction,
    abortTransaction,
    commitOffsetMessageTransaction,
    TxError,
    getKafkaError,
    kafkaErrorIsFatal,
    kafkaErrorIsRetriable,
    kafkaErrorTxnRequiresAbort,

    -- * Raw handle escape hatch
    askProducerHandle,

    -- * Types
    ProducerRecord (..),
    ProducePartition (..),
    DeliveryReport (..),
    ImmediateError (..),

    -- * Configuration
    ProducerProperties (..),
    K.brokersList,
    K.setCallback,
    K.logLevel,
    K.compression,
    K.topicCompression,
    K.sendTimeout,
    K.statisticsInterval,
    K.extraProps,
    K.extraProp,
    K.suppressDisconnectLogs,
    K.extraTopicProps,
    K.debugOptions,

    -- * Callbacks
    K.deliveryCallback,
    K.errorCallback,
    K.logCallback,
    K.statsCallback,
    K.Callback,

    -- * Common Types
    KafkaError (..),
    TopicName (..),
    BrokerAddress (..),
    Timeout (..),
    KafkaLogLevel (..),
    KafkaDebug (..),
    KafkaCompressionCodec (..),
    Headers,
    headersFromList,
    headersToList,
    Offset (..),
  )
where

-- Offset is in Consumer.Types
import Kafka.Consumer.Types (Offset (..))
import Kafka.Effectful.Producer.Effect
  ( KafkaProducer,
    abortTransaction,
    askProducerHandle,
    beginTransaction,
    commitTransaction,
    flushProducer,
    initTransactions,
    produceMessage,
    produceMessage',
    produceMessageBatch,
    produceMessageSync,
  )
import Kafka.Effectful.Producer.Interpreter (runKafkaProducer)
import Kafka.Effectful.Producer.Transaction (commitOffsetMessageTransaction)
import Kafka.Producer.ProducerProperties (ProducerProperties (..))
import Kafka.Producer.ProducerProperties qualified as K
import Kafka.Producer.Types (DeliveryReport (..), ImmediateError (..), ProducePartition (..), ProducerRecord (..))
import Kafka.Transaction
  ( TxError,
    getKafkaError,
    kafkaErrorIsFatal,
    kafkaErrorIsRetriable,
    kafkaErrorTxnRequiresAbort,
  )
import Kafka.Types (BrokerAddress (..), Headers, KafkaCompressionCodec (..), KafkaDebug (..), KafkaError (..), KafkaLogLevel (..), Timeout (..), TopicName (..), headersFromList, headersToList)
