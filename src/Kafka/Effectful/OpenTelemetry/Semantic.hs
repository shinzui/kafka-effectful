{- | Pure helpers that translate Kafka producer and consumer records
into the OpenTelemetry messaging-semantic-conventions attribute set.

The functions here do not perform any I\/O and do not depend on a
'OpenTelemetry.Trace.Core.Tracer'. They exist as building blocks for
the traced interpreters in
"Kafka.Effectful.OpenTelemetry.Producer.Interpreter" and
"Kafka.Effectful.OpenTelemetry.Consumer.Interpreter", and are also
exposed so users who want to write a custom interpreter or a
framework wrapper can produce the same spec-aligned attribute set
without having to redefine the keys themselves.

The attribute keys and value types follow the OpenTelemetry messaging
semantic conventions v1.40 as exposed by
@hs-opentelemetry-semantic-conventions@. Operation and consumer-group
keys follow @hs-opentelemetry-api@ 1.0\'s stability opt-in policy:
legacy by default, stable with @OTEL_SEMCONV_STABILITY_OPT_IN=messaging@,
and both with @OTEL_SEMCONV_STABILITY_OPT_IN=messaging\/dup@.

The design parallels the upstream
@hs-opentelemetry-instrumentation-hw-kafka-client@\'s
@OpenTelemetry.Instrumentation.Kafka@ module.

@since 0.2.0.0
-}
module Kafka.Effectful.OpenTelemetry.Semantic (
    -- * Constants
    kafkaMessagingSystem,
    producerOperationName,
    consumerOperationName,

    -- * Span names
    producerSpanName,
    consumerSpanName,

    -- * Attribute builders
    producerRecordAttributes,
    producerRecordAttributesWith,
    consumerRecordAttributes,
    consumerRecordAttributesWith,
)
where

import Data.ByteString (ByteString)
import Data.ByteString qualified as BS
import Data.Int (Int64)
import Data.Map.Strict qualified as Map
import Data.Text (Text)
import Data.Text.Encoding (decodeUtf8')
import Kafka.Consumer.ConsumerProperties (ConsumerProperties (cpProps))
import Kafka.Consumer.Types (
    ConsumerRecord (crKey, crOffset, crPartition, crTopic, crValue),
    Offset (unOffset),
 )
import Kafka.Producer.Types (
    ProducePartition (SpecifiedPartition, UnassignedPartition),
    ProducerRecord (prKey, prPartition, prTopic, prValue),
 )
import Kafka.Types (PartitionId (unPartitionId), TopicName (..))
import OpenTelemetry.Attributes.Attribute (toAttribute)
import OpenTelemetry.Attributes.Map (AttributeMap, insertAttributeByKey)
import OpenTelemetry.SemanticConventions (
    messaging_client_id,
    messaging_consumer_group_name,
    messaging_destination_name,
    messaging_kafka_consumer_group,
    messaging_kafka_destination_partition,
    messaging_kafka_message_key,
    messaging_kafka_message_offset,
    messaging_message_body_size,
    messaging_operation,
    messaging_operation_name,
    messaging_operation_type,
    messaging_system,
 )
import OpenTelemetry.SemanticsConfig (StabilityOpt (..))

{- | The constant @"kafka"@ used as the value of the
@messaging.system@ attribute.
-}
kafkaMessagingSystem :: Text
kafkaMessagingSystem = "kafka"

{- | The operation name carried by producer-side spans
(@\"send\"@). The full span name is built by 'producerSpanName'.
-}
producerOperationName :: Text
producerOperationName = "send"

{- | The operation name carried by consumer-side spans
(@\"process\"@). The full span name is built by 'consumerSpanName'.
-}
consumerOperationName :: Text
consumerOperationName = "process"

{- | Build the canonical producer span name @\"send \<topic\>\"@.

This is the name shape the OTel messaging semantic conventions
recommend for Producer-kind spans (e.g. @"send orders"@).
-}
producerSpanName :: TopicName -> Text
producerSpanName (TopicName t) = producerOperationName <> " " <> t

{- | Build the canonical consumer span name @\"process \<topic\>\"@.

This is the name shape the OTel messaging semantic conventions
recommend for Consumer-kind spans (e.g. @"process orders"@).
-}
consumerSpanName :: TopicName -> Text
consumerSpanName (TopicName t) = consumerOperationName <> " " <> t

{- | Build the OpenTelemetry attribute map for a Producer-kind span
describing a single 'ProducerRecord'.

The map always includes @messaging.system=kafka@,
@messaging.destination.name=<topic>@, and
@messaging.operation=send@. It also includes
@messaging.kafka.destination.partition@ (Int64) when the record
targets a 'SpecifiedPartition' (omitted for 'UnassignedPartition',
since the broker will pick the partition), and
@messaging.kafka.message.key@ (Text) when the record\'s @prKey@
is present and decodes as UTF-8.
-}
producerRecordAttributes :: ProducerRecord -> AttributeMap
producerRecordAttributes = producerRecordAttributesWith Old

{- | Build the OpenTelemetry attribute map for a Producer-kind span
using the supplied messaging semantic-convention stability mode.
-}
producerRecordAttributesWith ::
    StabilityOpt ->
    ProducerRecord ->
    AttributeMap
producerRecordAttributesWith semOpts record =
    addOperation semOpts
        . addDestination
        . addPartition
        . addKey
        . addBodySize
        . addSystem
        $ mempty
  where
    addSystem =
        insertAttributeByKey messaging_system $
            toAttribute kafkaMessagingSystem
    addOperation = \case
        Old ->
            insertAttributeByKey messaging_operation $
                toAttribute producerOperationName
        Stable ->
            insertAttributeByKey messaging_operation_name (toAttribute producerOperationName)
                . insertAttributeByKey messaging_operation_type (toAttribute producerOperationName)
        StableAndOld ->
            insertAttributeByKey messaging_operation (toAttribute producerOperationName)
                . insertAttributeByKey messaging_operation_name (toAttribute producerOperationName)
                . insertAttributeByKey messaging_operation_type (toAttribute producerOperationName)
    addDestination =
        insertAttributeByKey messaging_destination_name $
            toAttribute (unTopicName (prTopic record))
    addPartition = case prPartition record of
        SpecifiedPartition p ->
            insertAttributeByKey messaging_kafka_destination_partition $
                toAttribute (fromIntegral p :: Int64)
        UnassignedPartition -> id
    addKey = case prKey record >>= decodeKey of
        Just k ->
            insertAttributeByKey messaging_kafka_message_key $
                toAttribute k
        Nothing -> id
    addBodySize = case prValue record of
        Just v ->
            insertAttributeByKey messaging_message_body_size $
                toAttribute (fromIntegral (BS.length v) :: Int64)
        Nothing -> id

{- | Build the OpenTelemetry attribute map for a Consumer-kind span
describing a single 'ConsumerRecord'.

The map always includes @messaging.system=kafka@,
@messaging.destination.name=<topic>@,
@messaging.operation=process@,
@messaging.kafka.destination.partition@ (Int64), and
@messaging.kafka.message.offset@ (Int64). It also includes
@messaging.kafka.message.key@ (Text) when the record\'s @crKey@
is present and decodes as UTF-8.

Note: consumer-group and client-id attributes need
@Kafka.Consumer.ConsumerProperties@, not just the record. Use
'consumerRecordAttributesWith' when those properties are available.
-}
consumerRecordAttributes ::
    ConsumerRecord (Maybe ByteString) (Maybe ByteString) ->
    AttributeMap
consumerRecordAttributes = consumerRecordAttributesLegacy

consumerRecordAttributesLegacy ::
    ConsumerRecord (Maybe ByteString) (Maybe ByteString) ->
    AttributeMap
consumerRecordAttributesLegacy record =
    addConsumerCommon Old record

{- | Build the OpenTelemetry attribute map for a Consumer-kind span
using the supplied messaging semantic-convention stability mode and
consumer properties.
-}
consumerRecordAttributesWith ::
    StabilityOpt ->
    ConsumerProperties ->
    ConsumerRecord (Maybe ByteString) (Maybe ByteString) ->
    AttributeMap
consumerRecordAttributesWith semOpts props record =
    addConsumerGroup semOpts
        . addClientId
        $ addConsumerCommon semOpts record
  where
    addConsumerGroup = \case
        Old -> addOldConsumerGroup
        Stable -> addStableConsumerGroup
        StableAndOld -> addOldConsumerGroup . addStableConsumerGroup
    addOldConsumerGroup attrs =
        case Map.lookup "group.id" (cpProps props) of
            Just groupId ->
                insertAttributeByKey
                    messaging_kafka_consumer_group
                    (toAttribute groupId)
                    attrs
            Nothing -> attrs
    addStableConsumerGroup attrs =
        case Map.lookup "group.id" (cpProps props) of
            Just groupId ->
                insertAttributeByKey
                    messaging_consumer_group_name
                    (toAttribute groupId)
                    attrs
            Nothing -> attrs
    addClientId attrs =
        case Map.lookup "client.id" (cpProps props) of
            Just clientId ->
                insertAttributeByKey
                    messaging_client_id
                    (toAttribute clientId)
                    attrs
            Nothing -> attrs

addConsumerCommon ::
    StabilityOpt ->
    ConsumerRecord (Maybe ByteString) (Maybe ByteString) ->
    AttributeMap
addConsumerCommon semOpts record =
    addOperation semOpts
        . addDestination
        . addPartition
        . addOffset
        . addKey
        . addBodySize
        . addSystem
        $ mempty
  where
    addSystem =
        insertAttributeByKey messaging_system $
            toAttribute kafkaMessagingSystem
    addOperation = \case
        Old ->
            insertAttributeByKey messaging_operation $
                toAttribute consumerOperationName
        Stable ->
            insertAttributeByKey messaging_operation_name (toAttribute consumerOperationName)
                . insertAttributeByKey messaging_operation_type (toAttribute consumerOperationName)
        StableAndOld ->
            insertAttributeByKey messaging_operation (toAttribute consumerOperationName)
                . insertAttributeByKey messaging_operation_name (toAttribute consumerOperationName)
                . insertAttributeByKey messaging_operation_type (toAttribute consumerOperationName)
    addDestination =
        insertAttributeByKey messaging_destination_name $
            toAttribute (unTopicName (crTopic record))
    addPartition =
        insertAttributeByKey messaging_kafka_destination_partition $
            toAttribute (fromIntegral (unPartitionId (crPartition record)) :: Int64)
    addOffset =
        insertAttributeByKey messaging_kafka_message_offset $
            toAttribute (unOffset (crOffset record))
    addKey = case crKey record >>= decodeKey of
        Just k ->
            insertAttributeByKey messaging_kafka_message_key $
                toAttribute k
        Nothing -> id
    addBodySize = case crValue record of
        Just v ->
            insertAttributeByKey messaging_message_body_size $
                toAttribute (fromIntegral (BS.length v) :: Int64)
        Nothing -> id

{- | Decode message-key bytes as UTF-8, returning 'Nothing' if the
bytes are not valid UTF-8. Matches the behavior of upstream
@OpenTelemetry.Instrumentation.Kafka@\'s @prKey \/ crKey@ handling.
-}
decodeKey :: ByteString -> Maybe Text
decodeKey bs = case decodeUtf8' bs of
    Right t -> Just t
    Left _ -> Nothing
