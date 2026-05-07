module Main (main) where

import Kafka.Effectful.OpenTelemetry.PropagationTest qualified as PropagationTest
import Kafka.Effectful.OpenTelemetry.SemanticTest qualified as SemanticTest
import Kafka.Effectful.OpenTelemetry.ShibuyaCompatibilityTest qualified as ShibuyaCompatibilityTest
import Test.Tasty (TestTree, defaultMain, testGroup)

main :: IO ()
main = defaultMain tests

tests :: TestTree
tests =
    testGroup
        "Kafka.Effectful.OpenTelemetry"
        [ SemanticTest.tests
        , PropagationTest.tests
        , ShibuyaCompatibilityTest.tests
        ]
