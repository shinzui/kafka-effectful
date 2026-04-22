-- The 'EffectHandler' type synonym in effectful-core expands to a
-- constraint that GHC's redundant-constraint check flags on the
-- handler's signature, even though the constraint is required for
-- 'interpret' to type-check. Suppress the warning at the file level.
{-# OPTIONS_GHC -Wno-redundant-constraints #-}

module Kafka.Effectful.Producer.Interpreter (
    -- * Interpreter
    runKafkaProducer,
)
where

import Control.Concurrent.MVar qualified as Concurrent
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
runKafkaProducer props action =
    Exception.bracket
        acquire
        (Effectful.liftIO . K.closeProducer)
        (\producer -> interpret (handleProducer producer) action)
  where
    acquire = do
        result <- Effectful.liftIO $ K.newProducer props
        case result of
            Left err -> throwError err
            Right producer -> pure producer

handleProducer ::
    (IOE :> es, Error KafkaError :> es) =>
    K.KafkaProducer ->
    EffectHandler KafkaProducer es
handleProducer producer _env = \case
    ProduceMessage record -> do
        mbErr <- Effectful.liftIO $ K.produceMessage producer record
        for_ mbErr throwError
    ProduceMessage' record cb -> do
        res <- Effectful.liftIO $ K.produceMessage' producer record cb
        case res of
            Left (K.ImmediateError err) -> throwError err
            Right () -> pure ()
    ProduceMessageBatch records -> Effectful.liftIO $ do
        -- Hackage hw-kafka-client-5.3.0 does not export
        -- 'Kafka.Producer.produceMessageBatch', so we inline the same
        -- definition it ships on master: mapM over the list and keep
        -- only the records that failed to enqueue.
        results <- mapM (\r -> (r,) <$> K.produceMessage producer r) records
        pure [(r, err) | (r, Just err) <- results]
    ProduceMessageSync record -> do
        var <- Effectful.liftIO Concurrent.newEmptyMVar
        res <-
            Effectful.liftIO $
                K.produceMessage' producer record (Concurrent.putMVar var)
        case res of
            Left (K.ImmediateError err) -> throwError err
            Right () -> do
                Effectful.liftIO $ K.flushProducer producer
                report <- Effectful.liftIO $ Concurrent.takeMVar var
                case report of
                    K.DeliverySuccess _ offset -> pure offset
                    K.DeliveryFailure _ err -> throwError err
                    K.NoMessageError err -> throwError err
    FlushProducer ->
        Effectful.liftIO $ K.flushProducer producer
