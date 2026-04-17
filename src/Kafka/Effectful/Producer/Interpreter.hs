module Kafka.Effectful.Producer.Interpreter (
    -- * Interpreter
    runKafkaProducer,
)
where

import Data.Foldable (for_)
import Effectful (Eff, IOE, (:>))
import Effectful qualified
import Effectful.Dispatch.Dynamic (EffectHandler, interpret)
import Effectful.Error.Static (Error, throwError)
import Effectful.Exception qualified as Exception
import Kafka.Effectful.Producer.Effect (KafkaProducer (..))
import Kafka.Producer qualified as K
import Kafka.Producer.ProducerProperties (ProducerProperties)
import Kafka.Types (KafkaError)

{- | Run the 'KafkaProducer' effect.

Acquires a producer handle from the given properties and releases it
when the effect scope ends. Errors are thrown via the 'Error' effect.
-}
runKafkaProducer ::
    (IOE :> es, Error KafkaError :> es) =>
    ProducerProperties ->
    Eff (KafkaProducer : es) a ->
    Eff es a
runKafkaProducer props action = do
    result <- Effectful.liftIO $ K.newProducer props
    case result of
        Left err -> throwError err
        Right producer ->
            Exception.bracket
                (pure producer)
                (Effectful.liftIO . K.closeProducer)
                (\p -> interpret (handleProducer p) action)

handleProducer ::
    (IOE :> es, Error KafkaError :> es) =>
    K.KafkaProducer ->
    EffectHandler KafkaProducer es
handleProducer producer _env = \case
    ProduceMessage record -> do
        mbErr <- Effectful.liftIO $ K.produceMessage producer record
        for_ mbErr throwError
    FlushProducer ->
        Effectful.liftIO $ K.flushProducer producer
