module Kafka.Effectful.Producer.Effect (
    -- * Effect
    KafkaProducer (..),

    -- * Operations
    produceMessage,
    produceMessage',
    produceMessageSync,
    flushProducer,
)
where

import Effectful (Dispatch (..), DispatchOf, Eff, Effect, (:>))
import Effectful.Dispatch.Dynamic (send)
import Kafka.Consumer.Types (Offset)
import Kafka.Producer.Types (DeliveryReport, ProducerRecord)

-- | Effect for Kafka producer operations.
data KafkaProducer :: Effect where
    ProduceMessage ::
        ProducerRecord ->
        KafkaProducer m ()
    ProduceMessage' ::
        ProducerRecord ->
        (DeliveryReport -> IO ()) ->
        KafkaProducer m ()
    ProduceMessageSync ::
        ProducerRecord ->
        KafkaProducer m Offset
    FlushProducer ::
        KafkaProducer m ()

type instance DispatchOf KafkaProducer = 'Dynamic

{- | Send a single message to Kafka.
Throws 'KafkaError' via the 'Error' effect on failure.
-}
produceMessage :: (KafkaProducer :> es) => ProducerRecord -> Eff es ()
produceMessage = send . ProduceMessage

{- | Send a single message with a per-message 'DeliveryReport' callback.

The callback runs on a librdkafka-forked thread, so blocking operations
(such as writing to an 'MVar') are safe. Throws 'KafkaError' via the
'Error' effect when the underlying send fails to enqueue
(@ImmediateError@).

This is the low-level primitive for Scenario 2 of
@hw-kafka-client@'s producer best practices. Callers that only need a
single synchronous send should prefer 'produceMessageSync'.

@since 0.2.0.0
-}
produceMessage' ::
    (KafkaProducer :> es) =>
    ProducerRecord ->
    (DeliveryReport -> IO ()) ->
    Eff es ()
produceMessage' record cb = send (ProduceMessage' record cb)

{- | Send a single message and block until the broker acknowledges it,
returning the broker-assigned 'Offset'.

Throws 'KafkaError' via the 'Error' effect on enqueue failure
(@ImmediateError@) or on delivery failure reported via the
'DeliveryReport'.

This is the high-level convenience for Scenario 2 of
@hw-kafka-client@'s producer best practices.

@since 0.2.0.0
-}
produceMessageSync ::
    (KafkaProducer :> es) =>
    ProducerRecord ->
    Eff es Offset
produceMessageSync = send . ProduceMessageSync

-- | Flush the producer's outbound queue, blocking until all messages are sent.
flushProducer :: (KafkaProducer :> es) => Eff es ()
flushProducer = send FlushProducer
