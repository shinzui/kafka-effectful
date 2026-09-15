module Main (main) where

import Kafka.Effectful.Consumer.ClassifyTest qualified as ClassifyTest
import Kafka.Effectful.Consumer.InterpreterTest qualified as InterpreterTest
import Kafka.Effectful.OpenTelemetry.ConsumerSpanTest qualified as ConsumerSpanTest
import Kafka.Effectful.OpenTelemetry.PropagationTest qualified as PropagationTest
import Kafka.Effectful.OpenTelemetry.SemanticTest qualified as SemanticTest
import Kafka.Effectful.OpenTelemetry.ShibuyaCompatibilityTest qualified as ShibuyaCompatibilityTest
import Test.Tasty (TestTree, defaultMain, testGroup)

main :: IO ()
main = defaultMain tests

tests :: TestTree
tests =
  testGroup
    "kafka-effectful"
    [ testGroup
        "Kafka.Effectful.OpenTelemetry"
        [ SemanticTest.tests,
          PropagationTest.tests,
          ShibuyaCompatibilityTest.tests,
          ConsumerSpanTest.tests
        ],
      testGroup
        "Kafka.Effectful.Consumer"
        [ ClassifyTest.tests,
          InterpreterTest.tests
        ]
    ]
