-- The 'EffectHandler' type synonym in effectful-core expands to a
-- constraint that GHC's redundant-constraint check flags on the
-- handler's signature, even though the constraint is required for
-- 'interpret' to type-check. Suppress the warning at the file level.
{-# OPTIONS_GHC -Wno-redundant-constraints #-}

module Kafka.Effectful.Consumer.Interpreter (
    -- * Interpreter
    runKafkaConsumer,
)
where

import Control.Monad (void)
import Data.Foldable (for_)
import Effectful (Eff, IOE, (:>))
import Effectful qualified
import Effectful.Dispatch.Dynamic (EffectHandler, interpret)
import Effectful.Error.Static (Error, throwError)
import Effectful.Exception (ExitCase (..))
import Effectful.Exception qualified as Exception
import Kafka.Consumer (RdKafkaRespErrT (..))
import Kafka.Consumer qualified as K
import Kafka.Consumer.ConsumerProperties (ConsumerProperties)
import Kafka.Consumer.Subscription (Subscription)
import Kafka.Effectful.Consumer.Effect (KafkaConsumer (..))
import Kafka.Types (KafkaError (..))

{- | Run the 'KafkaConsumer' effect.

Acquires a consumer handle from the given properties and subscription,
and releases it when the effect scope ends. Errors are thrown via the
'Error' effect.
-}
runKafkaConsumer ::
    (IOE :> es, Error KafkaError :> es) =>
    ConsumerProperties ->
    Subscription ->
    Eff (KafkaConsumer : es) a ->
    Eff es a
runKafkaConsumer props sub action =
    fst
        <$> Exception.generalBracket
            acquire
            release
            (\consumer -> interpret (handleConsumer consumer) action)
  where
    acquire = do
        result <- Effectful.liftIO $ K.newConsumer props sub
        case result of
            Left err -> throwError err
            Right consumer -> pure consumer

    release consumer = \case
        ExitCaseSuccess _ -> do
            mbErr <- Effectful.liftIO $ K.closeConsumer consumer
            for_ mbErr throwError
        ExitCaseException _ ->
            Effectful.liftIO . void $ K.closeConsumer consumer
        ExitCaseAbort ->
            Effectful.liftIO . void $ K.closeConsumer consumer

handleConsumer ::
    (IOE :> es, Error KafkaError :> es) =>
    K.KafkaConsumer ->
    EffectHandler KafkaConsumer es
handleConsumer consumer _env = \case
    PollMessage timeout -> do
        result <- Effectful.liftIO $ K.pollMessage consumer timeout
        case result of
            Left (KafkaResponseError RdKafkaRespErrTimedOut) -> pure Nothing
            Left err -> throwError err
            Right msg -> pure (Just msg)
    PollMessageBatch timeout batchSize ->
        Effectful.liftIO $ K.pollMessageBatch consumer timeout batchSize
    CommitOffsetMessage oc cr -> throwOnJust $ K.commitOffsetMessage oc consumer cr
    CommitAllOffsets oc -> throwOnJust $ K.commitAllOffsets oc consumer
    CommitPartitionsOffsets oc tps -> throwOnJust $ K.commitPartitionsOffsets oc consumer tps
    StoreOffsets tps -> throwOnJust $ K.storeOffsets consumer tps
    StoreOffsetMessage cr -> throwOnJust $ K.storeOffsetMessage consumer cr
    Assign tps -> throwOnJust $ K.assign consumer tps
    PausePartitions parts ->
        throwOnKafkaErr (K.pausePartitions consumer parts)
    ResumePartitions parts ->
        throwOnKafkaErr (K.resumePartitions consumer parts)
    SeekPartitions tps timeout -> throwOnJust $ K.seekPartitions consumer tps timeout
    Committed timeout parts -> throwOnLeft $ K.committed consumer timeout parts
    Position parts -> throwOnLeft $ K.position consumer parts
    Assignment -> throwOnLeft $ K.assignment consumer
    Subscription -> throwOnLeft $ K.subscription consumer
    AskConsumerHandle -> pure consumer
  where
    throwOnJust action' = do
        mbErr <- Effectful.liftIO action'
        for_ mbErr throwError

    throwOnLeft action' = do
        result <- Effectful.liftIO action'
        case result of
            Left err -> throwError err
            Right a -> pure a

    throwOnKafkaErr action' = do
        err <- Effectful.liftIO action'
        case err of
            KafkaResponseError RdKafkaRespErrNoError -> pure ()
            _ -> throwError err
