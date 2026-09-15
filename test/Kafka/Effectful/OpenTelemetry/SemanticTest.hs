module Kafka.Effectful.OpenTelemetry.SemanticTest (tests) where

import Data.ByteString (ByteString)
import Data.ByteString qualified as BS
import Data.HashMap.Lazy qualified as HashMap
import Data.Int (Int64)
import Data.Text (Text)
import Kafka.Consumer.ConsumerProperties qualified as ConsumerProperties
import Kafka.Consumer.Types
  ( ConsumerGroupId (..),
    ConsumerRecord (..),
    Offset (..),
    Timestamp (NoTimestamp),
  )
import Kafka.Effectful.OpenTelemetry.Semantic
  ( consumerRecordAttributes,
    consumerRecordAttributesWith,
    producerRecordAttributes,
    producerRecordAttributesWith,
  )
import Kafka.Producer.Types
  ( ProducePartition (SpecifiedPartition, UnassignedPartition),
    ProducerRecord (..),
  )
import Kafka.Types
  ( ClientId (..),
    PartitionId (..),
    TopicName (..),
    headersFromList,
  )
import OpenTelemetry.Attributes.Attribute
  ( Attribute (AttributeValue),
    PrimitiveAttribute (IntAttribute, TextAttribute),
  )
import OpenTelemetry.SemanticsConfig (StabilityOpt (..))
import Test.Tasty (TestTree, testGroup)
import Test.Tasty.HUnit (assertFailure, testCase, (@?=))

tests :: TestTree
tests =
  testGroup
    "Semantic"
    [ producerTests,
      consumerTests
    ]

producerTests :: TestTree
producerTests =
  testGroup
    "producerRecordAttributes"
    [ testCase "legacy helper keeps old operation key" $ do
        let attrs = producerRecordAttributes (sampleProducerRecord (Just "k1"))
        HashMap.lookup "messaging.system" attrs @?= Just (textAttr "kafka")
        HashMap.lookup "messaging.destination.name" attrs @?= Just (textAttr "orders")
        HashMap.lookup "messaging.operation" attrs @?= Just (textAttr "send")
        assertAbsent "messaging.operation.name" attrs
        assertAbsent "messaging.operation.type" attrs,
      testCase "stable mode uses stable operation keys" $ do
        let attrs = producerRecordAttributesWith Stable (sampleProducerRecord (Just "k1"))
        HashMap.lookup "messaging.operation.name" attrs @?= Just (textAttr "send")
        HashMap.lookup "messaging.operation.type" attrs @?= Just (textAttr "send")
        assertAbsent "messaging.operation" attrs,
      testCase "duplicate mode emits old and stable operation keys" $ do
        let attrs = producerRecordAttributesWith StableAndOld (sampleProducerRecord (Just "k1"))
        HashMap.lookup "messaging.operation" attrs @?= Just (textAttr "send")
        HashMap.lookup "messaging.operation.name" attrs @?= Just (textAttr "send")
        HashMap.lookup "messaging.operation.type" attrs @?= Just (textAttr "send"),
      testCase "adds partition, key, and body size when available" $ do
        let attrs = producerRecordAttributesWith Old (sampleProducerRecord (Just "hello"))
        HashMap.lookup "messaging.kafka.destination.partition" attrs
          @?= Just (intAttr 42)
        HashMap.lookup "messaging.kafka.message.key" attrs
          @?= Just (textAttr "hello")
        HashMap.lookup "messaging.message.body.size" attrs
          @?= Just (intAttr 5),
      testCase "omits partition for UnassignedPartition" $ do
        let attrs =
              producerRecordAttributesWith
                Old
                (sampleProducerRecord (Just "k1"))
                  { prPartition = UnassignedPartition
                  }
        assertAbsent "messaging.kafka.destination.partition" attrs,
      testCase "omits invalid or absent keys" $ do
        let invalidAttrs =
              producerRecordAttributesWith
                Old
                (sampleProducerRecord (Just (BS.pack [0xC3, 0x28])))
            missingAttrs =
              producerRecordAttributesWith Old (sampleProducerRecord Nothing)
        assertAbsent "messaging.kafka.message.key" invalidAttrs
        assertAbsent "messaging.kafka.message.key" missingAttrs
    ]

consumerTests :: TestTree
consumerTests =
  testGroup
    "consumerRecordAttributes"
    [ testCase "legacy helper keeps old operation key" $ do
        let attrs = consumerRecordAttributes (sampleConsumerRecord (Just "k1"))
        HashMap.lookup "messaging.system" attrs @?= Just (textAttr "kafka")
        HashMap.lookup "messaging.destination.name" attrs @?= Just (textAttr "orders")
        HashMap.lookup "messaging.operation" attrs @?= Just (textAttr "process")
        assertAbsent "messaging.operation.name" attrs
        assertAbsent "messaging.operation.type" attrs,
      testCase "old mode uses legacy consumer group key" $ do
        let attrs = consumerRecordAttributesWith Old sampleConsumerProps (sampleConsumerRecord (Just "k1"))
        HashMap.lookup "messaging.kafka.consumer.group" attrs
          @?= Just (textAttr "orders-group")
        assertAbsent "messaging.consumer.group.name" attrs,
      testCase "stable mode uses stable operation and consumer group keys" $ do
        let attrs = consumerRecordAttributesWith Stable sampleConsumerProps (sampleConsumerRecord (Just "k1"))
        HashMap.lookup "messaging.operation.name" attrs @?= Just (textAttr "process")
        HashMap.lookup "messaging.operation.type" attrs @?= Just (textAttr "process")
        HashMap.lookup "messaging.consumer.group.name" attrs
          @?= Just (textAttr "orders-group")
        assertAbsent "messaging.operation" attrs
        assertAbsent "messaging.kafka.consumer.group" attrs,
      testCase "duplicate mode emits old and stable operation and group keys" $ do
        let attrs = consumerRecordAttributesWith StableAndOld sampleConsumerProps (sampleConsumerRecord (Just "k1"))
        HashMap.lookup "messaging.operation" attrs @?= Just (textAttr "process")
        HashMap.lookup "messaging.operation.name" attrs @?= Just (textAttr "process")
        HashMap.lookup "messaging.operation.type" attrs @?= Just (textAttr "process")
        HashMap.lookup "messaging.kafka.consumer.group" attrs
          @?= Just (textAttr "orders-group")
        HashMap.lookup "messaging.consumer.group.name" attrs
          @?= Just (textAttr "orders-group"),
      testCase "adds client id, partition, offset, key, and body size" $ do
        let attrs = consumerRecordAttributesWith Old sampleConsumerProps (sampleConsumerRecord (Just "hello"))
        HashMap.lookup "messaging.client.id" attrs
          @?= Just (textAttr "orders-client")
        HashMap.lookup "messaging.kafka.destination.partition" attrs
          @?= Just (intAttr 7)
        HashMap.lookup "messaging.kafka.message.offset" attrs
          @?= Just (intAttr 42)
        HashMap.lookup "messaging.kafka.message.key" attrs
          @?= Just (textAttr "hello")
        HashMap.lookup "messaging.message.body.size" attrs
          @?= Just (intAttr 5),
      testCase "omits invalid or absent keys" $ do
        let invalidAttrs =
              consumerRecordAttributesWith
                Old
                sampleConsumerProps
                (sampleConsumerRecord (Just (BS.pack [0xC3, 0x28])))
            missingAttrs =
              consumerRecordAttributesWith Old sampleConsumerProps (sampleConsumerRecord Nothing)
        assertAbsent "messaging.kafka.message.key" invalidAttrs
        assertAbsent "messaging.kafka.message.key" missingAttrs
    ]

sampleProducerRecord :: Maybe ByteString -> ProducerRecord
sampleProducerRecord key =
  ProducerRecord
    { prTopic = TopicName "orders",
      prPartition = SpecifiedPartition 42,
      prKey = key,
      prValue = Just "value",
      prHeaders = headersFromList []
    }

sampleConsumerRecord ::
  Maybe ByteString ->
  ConsumerRecord (Maybe ByteString) (Maybe ByteString)
sampleConsumerRecord key =
  ConsumerRecord
    { crTopic = TopicName "orders",
      crPartition = PartitionId 7,
      crOffset = Offset 42,
      crTimestamp = NoTimestamp,
      crHeaders = headersFromList [],
      crKey = key,
      crValue = Just "value"
    }

sampleConsumerProps :: ConsumerProperties.ConsumerProperties
sampleConsumerProps =
  ConsumerProperties.groupId (ConsumerGroupId "orders-group")
    <> ConsumerProperties.clientId (ClientId "orders-client")

textAttr :: Text -> Attribute
textAttr = AttributeValue . TextAttribute

intAttr :: Int64 -> Attribute
intAttr = AttributeValue . IntAttribute

assertAbsent :: Text -> HashMap.HashMap Text Attribute -> IO ()
assertAbsent key attrs =
  case HashMap.lookup key attrs of
    Nothing -> pure ()
    Just attr ->
      assertFailure $
        "expected "
          <> show key
          <> " to be absent, got "
          <> show attr
