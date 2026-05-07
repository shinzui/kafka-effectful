module Kafka.Effectful.OpenTelemetry.SemanticTest (tests) where

import Data.ByteString (ByteString)
import Data.ByteString qualified as BS
import Data.HashMap.Strict qualified as HashMap
import Data.Int (Int64)
import Data.Text (Text)
import Kafka.Consumer.Types (
    ConsumerRecord (..),
    Offset (..),
    Timestamp (NoTimestamp),
 )
import Kafka.Effectful.OpenTelemetry.Semantic (
    consumerRecordAttributes,
    producerRecordAttributes,
 )
import Kafka.Producer.Types (
    ProducePartition (SpecifiedPartition, UnassignedPartition),
    ProducerRecord (..),
 )
import Kafka.Types (
    PartitionId (..),
    TopicName (..),
    headersFromList,
 )
import OpenTelemetry.Attributes.Attribute (
    Attribute (AttributeValue),
    PrimitiveAttribute (IntAttribute, TextAttribute),
 )
import Test.Tasty (TestTree, testGroup)
import Test.Tasty.HUnit (assertEqual, assertFailure, testCase, (@?=))

tests :: TestTree
tests =
    testGroup
        "Semantic"
        [ producerTests
        , consumerTests
        ]

-- Producer-side attribute coverage

producerTests :: TestTree
producerTests =
    testGroup
        "producerRecordAttributes"
        [ testCase "adds messaging.system=kafka" $ do
            let attrs = producerRecordAttributes (sampleProducerRecord (Just "k1"))
            HashMap.lookup "messaging.system" attrs
                @?= Just (textAttr "kafka")
        , testCase "adds messaging.destination.name from topic" $ do
            let attrs = producerRecordAttributes (sampleProducerRecord (Just "k1"))
            HashMap.lookup "messaging.destination.name" attrs
                @?= Just (textAttr "orders")
        , testCase "adds messaging.operation=send" $ do
            let attrs = producerRecordAttributes (sampleProducerRecord (Just "k1"))
            HashMap.lookup "messaging.operation" attrs
                @?= Just (textAttr "send")
        , testCase "adds messaging.kafka.destination.partition for SpecifiedPartition" $ do
            let attrs = producerRecordAttributes (sampleProducerRecord (Just "k1"))
            HashMap.lookup "messaging.kafka.destination.partition" attrs
                @?= Just (intAttr 42)
        , testCase "omits messaging.kafka.destination.partition for UnassignedPartition" $ do
            let attrs =
                    producerRecordAttributes
                        (sampleProducerRecord (Just "k1"))
                            { prPartition = UnassignedPartition
                            }
            assertEqual
                "partition attribute should not be present"
                Nothing
                (HashMap.lookup "messaging.kafka.destination.partition" attrs)
        , testCase "adds messaging.kafka.message.key when valid UTF-8" $ do
            let attrs = producerRecordAttributes (sampleProducerRecord (Just "hello"))
            HashMap.lookup "messaging.kafka.message.key" attrs
                @?= Just (textAttr "hello")
        , testCase "omits messaging.kafka.message.key when invalid UTF-8" $ do
            let attrs =
                    producerRecordAttributes
                        (sampleProducerRecord (Just (BS.pack [0xC3, 0x28])))
            case HashMap.lookup "messaging.kafka.message.key" attrs of
                Nothing -> pure ()
                Just _ ->
                    assertFailure
                        "expected messaging.kafka.message.key to be omitted for invalid UTF-8"
        , testCase "omits messaging.kafka.message.key when key is Nothing" $ do
            let attrs = producerRecordAttributes (sampleProducerRecord Nothing)
            assertEqual
                "key attribute should not be present"
                Nothing
                (HashMap.lookup "messaging.kafka.message.key" attrs)
        ]

-- Consumer-side attribute coverage

consumerTests :: TestTree
consumerTests =
    testGroup
        "consumerRecordAttributes"
        [ testCase "adds messaging.system=kafka" $ do
            let attrs = consumerRecordAttributes (sampleConsumerRecord (Just "k1"))
            HashMap.lookup "messaging.system" attrs
                @?= Just (textAttr "kafka")
        , testCase "adds messaging.destination.name from topic" $ do
            let attrs = consumerRecordAttributes (sampleConsumerRecord (Just "k1"))
            HashMap.lookup "messaging.destination.name" attrs
                @?= Just (textAttr "orders")
        , testCase "adds messaging.operation=process" $ do
            let attrs = consumerRecordAttributes (sampleConsumerRecord (Just "k1"))
            HashMap.lookup "messaging.operation" attrs
                @?= Just (textAttr "process")
        , testCase "adds messaging.kafka.destination.partition" $ do
            let attrs = consumerRecordAttributes (sampleConsumerRecord (Just "k1"))
            HashMap.lookup "messaging.kafka.destination.partition" attrs
                @?= Just (intAttr 7)
        , testCase "adds messaging.kafka.message.offset" $ do
            let attrs = consumerRecordAttributes (sampleConsumerRecord (Just "k1"))
            HashMap.lookup "messaging.kafka.message.offset" attrs
                @?= Just (intAttr 42)
        , testCase "adds messaging.kafka.message.key when valid UTF-8" $ do
            let attrs = consumerRecordAttributes (sampleConsumerRecord (Just "hello"))
            HashMap.lookup "messaging.kafka.message.key" attrs
                @?= Just (textAttr "hello")
        , testCase "omits messaging.kafka.message.key when invalid UTF-8" $ do
            let attrs =
                    consumerRecordAttributes
                        (sampleConsumerRecord (Just (BS.pack [0xC3, 0x28])))
            case HashMap.lookup "messaging.kafka.message.key" attrs of
                Nothing -> pure ()
                Just _ ->
                    assertFailure
                        "expected messaging.kafka.message.key to be omitted for invalid UTF-8"
        , testCase "omits messaging.kafka.message.key when key is Nothing" $ do
            let attrs = consumerRecordAttributes (sampleConsumerRecord Nothing)
            assertEqual
                "key attribute should not be present"
                Nothing
                (HashMap.lookup "messaging.kafka.message.key" attrs)
        ]

-- Helpers

sampleProducerRecord :: Maybe ByteString -> ProducerRecord
sampleProducerRecord key =
    ProducerRecord
        { prTopic = TopicName "orders"
        , prPartition = SpecifiedPartition 42
        , prKey = key
        , prValue = Just "value"
        , prHeaders = headersFromList []
        }

sampleConsumerRecord ::
    Maybe ByteString ->
    ConsumerRecord (Maybe ByteString) (Maybe ByteString)
sampleConsumerRecord key =
    ConsumerRecord
        { crTopic = TopicName "orders"
        , crPartition = PartitionId 7
        , crOffset = Offset 42
        , crTimestamp = NoTimestamp
        , crHeaders = headersFromList []
        , crKey = key
        , crValue = Just "value"
        }

textAttr :: Text -> Attribute
textAttr = AttributeValue . TextAttribute

intAttr :: Int64 -> Attribute
intAttr = AttributeValue . IntAttribute
