module Kafka.Effectful.Producer.Effect (
    -- * Effect
    KafkaProducer (..),

    -- * Operations
    produceMessage,
    flushProducer,
)
where

import Effectful (Dispatch (..), DispatchOf, Eff, Effect, (:>))
import Effectful.Dispatch.Dynamic (send)
import Kafka.Producer.Types (ProducerRecord)

-- | Effect for Kafka producer operations.
data KafkaProducer :: Effect where
    ProduceMessage :: ProducerRecord -> KafkaProducer m ()
    FlushProducer :: KafkaProducer m ()

type instance DispatchOf KafkaProducer = 'Dynamic

{- | Send a single message to Kafka.
Throws 'KafkaError' via the 'Error' effect on failure.
-}
produceMessage :: (KafkaProducer :> es) => ProducerRecord -> Eff es ()
produceMessage = send . ProduceMessage

-- | Flush the producer's outbound queue, blocking until all messages are sent.
flushProducer :: (KafkaProducer :> es) => Eff es ()
flushProducer = send FlushProducer
