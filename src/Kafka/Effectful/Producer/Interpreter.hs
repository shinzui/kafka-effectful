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
import Kafka.Transaction qualified as K
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
        -- This is a per-record loop, not a batch send, and it saves no
        -- network round-trips over calling produceMessage yourself. The
        -- value here is the batch-shaped signature and the failed-record
        -- result, not throughput.
        --
        -- hw-kafka-client exports no batch produce at all. It removed its
        -- Haskell-level produceMessageBatch in 72e6f6d (Oct 2021, before
        -- v5.3.0), and that function was itself a mapM over produceMessage.
        -- Real batching would need a binding for librdkafka's
        -- rd_kafka_produce_batch, which the package has never had.
        --
        -- Tracked as upstream issue 'hw-kafka-client-no-produce-batch-binding';
        -- run `mori upstream-issues show hw-kafka-client-no-produce-batch-binding`.
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
    InitTransactions timeout ->
        throwOnJust $ K.initTransactions producer timeout
    BeginTransaction ->
        throwOnJust $ K.beginTransaction producer
    CommitTransaction timeout ->
        Effectful.liftIO $ K.commitTransaction producer timeout
    AbortTransaction timeout ->
        throwOnJust $ K.abortTransaction producer timeout
    SendOffsetsToTransaction consumer record timeout ->
        Effectful.liftIO $
            K.commitOffsetMessageTransaction producer consumer record timeout
    AskProducerHandle ->
        pure producer
  where
    throwOnJust action' = do
        mbErr <- Effectful.liftIO action'
        for_ mbErr throwError
