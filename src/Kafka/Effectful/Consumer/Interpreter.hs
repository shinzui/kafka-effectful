module Kafka.Effectful.Consumer.Interpreter (
    -- * Interpreter
    runKafkaConsumer,
)
where

import Effectful (Eff, IOE, (:>))
import Effectful qualified
import Effectful.Dispatch.Dynamic (EffectHandler, interpret)
import Effectful.Error.Static (Error, throwError)
import Effectful.Exception qualified as Exception
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
runKafkaConsumer props sub action = do
    result <- Effectful.liftIO $ K.newConsumer props sub
    case result of
        Left err -> throwError err
        Right consumer ->
            Exception.bracket
                (pure consumer)
                ( \c -> do
                    mbErr <- Effectful.liftIO $ K.closeConsumer c
                    case mbErr of
                        Nothing -> pure ()
                        Just err -> throwError err
                )
                (\c -> interpret (handleConsumer c) action)

handleConsumer ::
    (IOE :> es, Error KafkaError :> es) =>
    K.KafkaConsumer ->
    EffectHandler KafkaConsumer es
handleConsumer consumer _env = \case
    PollMessage timeout -> do
        result <- Effectful.liftIO $ K.pollMessage consumer timeout
        case result of
            Left err -> throwError err
            Right msg -> pure msg
    PollMessageBatch timeout batchSize ->
        Effectful.liftIO $ K.pollMessageBatch consumer timeout batchSize
    CommitOffsetMessage oc cr -> throwOnJust $ K.commitOffsetMessage oc consumer cr
    CommitAllOffsets oc -> throwOnJust $ K.commitAllOffsets oc consumer
    CommitPartitionsOffsets oc tps -> throwOnJust $ K.commitPartitionsOffsets oc consumer tps
    StoreOffsets tps -> throwOnJust $ K.storeOffsets consumer tps
    StoreOffsetMessage cr -> throwOnJust $ K.storeOffsetMessage consumer cr
    Assign tps -> throwOnJust $ K.assign consumer tps
    PausePartitions parts -> do
        err <- Effectful.liftIO $ K.pausePartitions consumer parts
        case err of
            KafkaResponseError rdErr
                | rdErr == toEnum 0 -> pure ()
            _ -> throwError err
    ResumePartitions parts -> do
        err <- Effectful.liftIO $ K.resumePartitions consumer parts
        case err of
            KafkaResponseError rdErr
                | rdErr == toEnum 0 -> pure ()
            _ -> throwError err
    SeekPartitions tps timeout -> throwOnJust $ K.seekPartitions consumer tps timeout
    Committed timeout parts -> throwOnLeft $ K.committed consumer timeout parts
    Position parts -> throwOnLeft $ K.position consumer parts
    Assignment -> throwOnLeft $ K.assignment consumer
    Subscription -> throwOnLeft $ K.subscription consumer
  where
    throwOnJust action' = do
        mbErr <- Effectful.liftIO action'
        case mbErr of
            Nothing -> pure ()
            Just err -> throwError err

    throwOnLeft action' = do
        result <- Effectful.liftIO action'
        case result of
            Left err -> throwError err
            Right a -> pure a
