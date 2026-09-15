-- | Scenario 5 of @producer-best-practices.md@: consume from one topic,
-- uppercase the value, and produce to another topic — all inside a
-- producer transaction, with consumer offsets committed as part of the
-- same transaction.
--
-- Assumes a Kafka broker at localhost:9092 with topics @source@ and
-- @destination@ already created (or auto-create enabled).
--
-- To exercise the exactly-once guarantee:
--
--   1. seed records into @source@ (e.g. @kcat -P -b localhost:9092 -t source@)
--   2. run this example; watch @destination@ (@kcat -C -e -b localhost:9092 -t destination@)
--   3. kill -9 the process mid-batch, restart it, and confirm no duplicates.
module Main (main) where

import Control.Monad (forever, unless)
import Data.ByteString (ByteString)
import Data.ByteString.Char8 qualified as BS8
import Data.Char (toUpper)
import Data.Either (rights)
import Data.Foldable (for_)
import Data.Map.Strict qualified as Map
import Effectful (Eff, IOE, liftIO, runEff, (:>))
import Effectful.Error.Static (Error, runError, throwError)
import Kafka.Effectful
import Kafka.Effectful.Consumer qualified as C
import Kafka.Effectful.Producer qualified as P
import System.Exit (exitFailure)
import System.IO (hPutStrLn, stderr)

brokerHost :: BrokerAddress
brokerHost = BrokerAddress "localhost:9092"

sourceTopic, destinationTopic :: TopicName
sourceTopic = TopicName "source"
destinationTopic = TopicName "destination"

producerProps :: ProducerProperties
producerProps =
  P.brokersList [brokerHost]
    <> P.sendTimeout (Timeout 30000)
    <> P.extraProp "transactional.id" "kafka-effectful-etl-1"
    <> P.extraProp "enable.idempotence" "true"
    <> P.extraProp "acks" "all"

consumerProps :: ConsumerProperties
consumerProps =
  C.brokersList [brokerHost]
    <> C.groupId (ConsumerGroupId "kafka-effectful-etl-group")
    <> C.noAutoCommit
    <> C.extraProp "isolation.level" "read_committed"

sourceSubscription :: Subscription
sourceSubscription = topics [sourceTopic] <> offsetReset Earliest

-- | Uppercase ASCII bytes in the value; drop anything else.
transform ::
  ConsumerRecord (Maybe ByteString) (Maybe ByteString) ->
  ProducerRecord
transform msg =
  ProducerRecord
    { prTopic = destinationTopic,
      prPartition = UnassignedPartition,
      prKey = crKey msg,
      prValue = fmap (BS8.map toUpper) (crValue msg),
      prHeaders = mempty
    }

-- | Keep only the last record per source partition — that is the offset
-- to commit into the transaction.
lastPerPartition ::
  [ConsumerRecord (Maybe ByteString) (Maybe ByteString)] ->
  [ConsumerRecord (Maybe ByteString) (Maybe ByteString)]
lastPerPartition =
  Map.elems . Map.fromList . fmap (\r -> ((crTopic r, crPartition r), r))

handleTxResult ::
  (KafkaProducer :> es, Error KafkaError :> es, IOE :> es) =>
  Maybe TxError ->
  Eff es ()
handleTxResult Nothing = pure ()
handleTxResult (Just err)
  | kafkaErrorTxnRequiresAbort err = abortTransaction (Timeout 5000)
  | kafkaErrorIsRetriable err =
      liftIO $ hPutStrLn stderr "retriable tx error — retry the whole transaction"
  | kafkaErrorIsFatal err = throwError (getKafkaError err)
  | otherwise =
      liftIO $ hPutStrLn stderr $ "tx error: " <> show (getKafkaError err)

etlLoop ::
  (KafkaProducer :> es, KafkaConsumer :> es, Error KafkaError :> es, IOE :> es) =>
  Eff es ()
etlLoop = do
  initTransactions (Timeout 10000)
  forever $ do
    msgs <- pollMessageBatch (Timeout 500) (BatchSize 100)
    let records = rights msgs
    unless (null records) $ do
      beginTransaction
      for_ records (produceMessage . transform)
      for_ (lastPerPartition records) $ \r ->
        commitOffsetMessageTransaction r (Timeout 5000)
          >>= handleTxResult
      commitTransaction (Timeout 5000) >>= handleTxResult

main :: IO ()
main = do
  result <-
    runEff . runError @KafkaError $
      runKafkaProducer producerProps $
        runKafkaConsumer consumerProps sourceSubscription etlLoop
  case result of
    Right () -> pure ()
    Left (_callStack, err) -> do
      hPutStrLn stderr $ "kafka error: " <> show err
      exitFailure
