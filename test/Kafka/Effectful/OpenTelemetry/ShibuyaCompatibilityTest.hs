module Kafka.Effectful.OpenTelemetry.ShibuyaCompatibilityTest (tests) where

import Data.HashMap.Strict qualified as HashMap
import Data.Int (Int64)
import Data.Text (Text)
import Kafka.Consumer.Types (
    ConsumerRecord (..),
    Offset (..),
    Timestamp (NoTimestamp),
 )
import Kafka.Effectful.OpenTelemetry.Semantic (consumerRecordAttributes)
import Kafka.Types (
    PartitionId (..),
    TopicName (..),
    headersFromList,
 )
import OpenTelemetry.Attributes (Attribute, toAttribute, unkey)
import OpenTelemetry.SemanticConventions (
    messaging_kafka_destination_partition,
    messaging_kafka_message_offset,
 )
import Test.Tasty (TestTree, testGroup)
import Test.Tasty.HUnit (assertEqual, testCase)

{- | Tests pinning @Kafka.Effectful.OpenTelemetry.Semantic@\'s
'consumerRecordAttributes' against the attribute set that
@shibuya-kafka-adapter@\'s
@Shibuya.Adapter.Kafka.Convert.kafkaSpanAttributes@ produces.

Both libraries depend on @hs-opentelemetry-semantic-conventions@,
so if upstream renames a key in a future release this test fails
together with shibuya — the desired failure mode for a
compatibility pin.
-}
tests :: TestTree
tests =
    testGroup
        "ShibuyaCompatibility"
        [ testCase "consumerRecordAttributes agrees on messaging.system" $ do
            let actual = HashMap.lookup "messaging.system" sampleAttrs
                expected = HashMap.lookup "messaging.system" shibuyaAttrs
            assertEqual "messaging.system attribute differs" expected actual
        , testCase "consumerRecordAttributes agrees on messaging.kafka.destination.partition" $ do
            let key = unkey messaging_kafka_destination_partition
                actual = HashMap.lookup key sampleAttrs
                expected = HashMap.lookup key shibuyaAttrs
            assertEqual "partition attribute differs" expected actual
        , testCase "consumerRecordAttributes agrees on messaging.kafka.message.offset" $ do
            let key = unkey messaging_kafka_message_offset
                actual = HashMap.lookup key sampleAttrs
                expected = HashMap.lookup key shibuyaAttrs
            assertEqual "offset attribute differs" expected actual
        ]

-- Sample inputs — partition 7, offset 42 — chosen so the resulting
-- attribute values are non-zero and easy to spot in failures.

samplePid :: PartitionId
samplePid = PartitionId 7

sampleOffset :: Offset
sampleOffset = Offset 42

sampleAttrs :: HashMap.HashMap Text Attribute
sampleAttrs =
    consumerRecordAttributes
        ConsumerRecord
            { crTopic = TopicName "orders"
            , crPartition = samplePid
            , crOffset = sampleOffset
            , crTimestamp = NoTimestamp
            , crHeaders = headersFromList []
            , crKey = Nothing
            , crValue = Just "value"
            }

{- | Local replica of @shibuya-kafka-adapter@\'s
@Shibuya.Adapter.Kafka.Convert.kafkaSpanAttributes@. Mirrors the
exact source-level expression at
@shibuya-kafka-adapter\/src\/Shibuya\/Adapter\/Kafka\/Convert.hs:78-89@:

> kafkaSpanAttributes :: PartitionId -> Offset -> HashMap Text Attribute
> kafkaSpanAttributes (PartitionId pid) (Offset off) =
>     HashMap.fromList
>         [ (\"messaging.system\", toAttribute (\"kafka\" :: Text))
>         , (unkey messaging_kafka_destination_partition, toAttribute (fromIntegral pid :: Int64))
>         , (unkey messaging_kafka_message_offset, toAttribute (off :: Int64))
>         ]
-}
shibuyaAttrs :: HashMap.HashMap Text Attribute
shibuyaAttrs =
    let PartitionId pid = samplePid
        Offset off = sampleOffset
     in HashMap.fromList
            [ ("messaging.system", toAttribute ("kafka" :: Text))
            ,
                ( unkey messaging_kafka_destination_partition
                , toAttribute (fromIntegral pid :: Int64)
                )
            ,
                ( unkey messaging_kafka_message_offset
                , toAttribute (off :: Int64)
                )
            ]
