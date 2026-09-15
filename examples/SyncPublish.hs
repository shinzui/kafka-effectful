-- | Scenario 2 of @producer-best-practices.md@: publish one record
-- synchronously and print the broker-assigned offset.
--
-- Assumes a Kafka broker at localhost:9092 with the topic
-- @kafka-effectful-sync-demo@ already created (or auto-create enabled).
-- Change the 'brokerHost' and 'topicName' bindings below if your local
-- broker lives somewhere else.
module Main (main) where

import Effectful (runEff)
import Effectful.Error.Static (runError)
import Kafka.Effectful
import Kafka.Effectful.Producer (brokersList, extraProp, sendTimeout)
import System.Exit (exitFailure)
import System.IO (hPutStrLn, stderr)

brokerHost :: BrokerAddress
brokerHost = BrokerAddress "localhost:9092"

topicName :: TopicName
topicName = TopicName "kafka-effectful-sync-demo"

producerProps :: ProducerProperties
producerProps =
  brokersList [brokerHost]
    <> sendTimeout (Timeout 10000)
    <> extraProp "enable.idempotence" "true"
    <> extraProp "acks" "all"

record :: ProducerRecord
record =
  ProducerRecord
    { prTopic = topicName,
      prPartition = UnassignedPartition,
      prKey = Just "demo-key",
      prValue = Just "hello from produceMessageSync",
      prHeaders = mempty
    }

main :: IO ()
main = do
  result <- runEff . runError @KafkaError $
    runKafkaProducer producerProps $ do
      produceMessageSync record
  case result of
    Right (Offset offset) ->
      putStrLn $ "delivered offset " <> show offset
    Left (_callStack, err) -> do
      hPutStrLn stderr $ "kafka error: " <> show err
      exitFailure
