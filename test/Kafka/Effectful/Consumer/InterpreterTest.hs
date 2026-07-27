{- | Interpreter-level tests that need no broker.

librdkafka connects lazily, so @newConsumer@ against an unreachable broker
address succeeds and returns a usable handle. That is enough to exercise the
operations whose defects are local to the interpreter rather than to any
broker interaction — in particular the idle-commit path, where
@commitAllOffsets@ with nothing assigned yields
@RdKafkaRespErrNoOffset@, which hw-kafka-client documents as not an error.

In-band benign poll classification (partition EOF, offset reset,
unknown topic) cannot be forced this way, because an offline client never
emits those conditions. That path is covered by
"Kafka.Effectful.Consumer.ClassifyTest" plus the fact that both interpreters
route through the same exported classifier.
-}
module Kafka.Effectful.Consumer.InterpreterTest (tests) where

import Effectful (Eff, IOE, runEff, (:>))
import Effectful.Error.Static (Error, runErrorNoCallStack)
import Kafka.Consumer.ConsumerProperties (
    ConsumerProperties,
    brokersList,
    groupId,
 )
import Kafka.Consumer.Subscription (Subscription, topics)
import Kafka.Consumer.Types (ConsumerGroupId (..), OffsetCommit (OffsetCommit))
import Kafka.Effectful.Consumer.Effect (
    KafkaConsumer,
    commitAllOffsets,
    pollMessage,
 )
import Kafka.Effectful.Consumer.Interpreter (runKafkaConsumer)
import Kafka.Types (
    BrokerAddress (..),
    KafkaError,
    Timeout (..),
    TopicName (..),
 )
import Test.Tasty (TestTree, testGroup)
import Test.Tasty.HUnit (assertFailure, testCase, (@?=))

{- | Point at a port nothing listens on. librdkafka will keep trying to
connect in the background; none of the operations under test wait for it.
-}
offlineProps :: ConsumerProperties
offlineProps =
    brokersList [BrokerAddress "localhost:1"]
        <> groupId (ConsumerGroupId "kafka-effectful-test-group")

offlineSubscription :: Subscription
offlineSubscription = topics [TopicName "kafka-effectful-test-topic"]

-- | Run an action under the plain interpreter against the offline consumer.
runOffline ::
    (forall es. (IOE :> es, Error KafkaError :> es, KafkaConsumer :> es) => Eff es a) ->
    IO (Either KafkaError a)
runOffline action =
    runEff . runErrorNoCallStack $
        runKafkaConsumer offlineProps offlineSubscription action

tests :: TestTree
tests =
    testGroup
        "Interpreter (brokerless)"
        [ -- Pins the half of pollMessage's contract that is not changing: a
          -- timeout is not an error.
          testCase "pollMessage returns Nothing on timeout" $ do
            result <- runOffline (pollMessage (Timeout 100))
            case result of
                Right Nothing -> pure ()
                Right (Just _) ->
                    assertFailure "expected no record from an offline consumer"
                Left err ->
                    assertFailure ("expected a timeout to be swallowed, got: " <> show err)
        , -- The KSC-5 regression. With nothing assigned there is nothing to
          -- commit, and librdkafka reports that as RdKafkaRespErrNoOffset --
          -- which hw-kafka-client's own callback documentation calls out as
          -- "not to be considered an error". Before the fix this threw, so an
          -- idle consumer could die on a shutdown commit.
          testCase "commitAllOffsets succeeds when there is nothing to commit" $ do
            result <- runOffline (commitAllOffsets OffsetCommit)
            result @?= Right ()
        ]
