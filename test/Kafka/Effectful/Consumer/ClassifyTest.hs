-- | Pins the in-band error taxonomy the consumer interpreters apply.
--
-- Laid out as label\/expectation tables deliberately, mirroring
-- @hw-kafka-streamly@\'s @test\/Kafka\/Streamly\/StreamTest.hs@, so the two
-- projects\' classifications of the same librdkafka codes can be read side by
-- side when either changes.
module Kafka.Effectful.Consumer.ClassifyTest (tests) where

import Kafka.Consumer (RdKafkaRespErrT (..))
import Kafka.Effectful.Consumer.Classify
  ( PollErrorDisposition (..),
    classifyPollError,
    isBenignCommitError,
  )
import Kafka.Types (KafkaError (..))
import Test.Tasty (TestTree, testGroup)
import Test.Tasty.HUnit (testCase, (@?=))

-- | Conditions a healthy consumer meets in normal operation. Swallowed.
benignPollErrors :: [(String, KafkaError)]
benignPollErrors =
  [ ("RdKafkaRespErrPartitionEof", KafkaResponseError RdKafkaRespErrPartitionEof),
    ("RdKafkaRespErrAutoOffsetReset", KafkaResponseError RdKafkaRespErrAutoOffsetReset),
    ("RdKafkaRespErrUnknownTopicOrPart", KafkaResponseError RdKafkaRespErrUnknownTopicOrPart)
  ]

-- | Conditions that must reach the caller as a thrown error.
-- 'RdKafkaRespErrFatal' is the important one: it is what librdkafka delivers
-- once a fatal error has been raised on the client, after which the consumer is
-- permanently dead.
throwingPollErrors :: [(String, KafkaError)]
throwingPollErrors =
  [ ("RdKafkaRespErrFatal", KafkaResponseError RdKafkaRespErrFatal),
    ("RdKafkaRespErrSaslAuthenticationFailed", KafkaResponseError RdKafkaRespErrSaslAuthenticationFailed),
    ("RdKafkaRespErrAuthentication", KafkaResponseError RdKafkaRespErrAuthentication),
    ("RdKafkaRespErrDestroy", KafkaResponseError RdKafkaRespErrDestroy),
    ("RdKafkaRespErrAllBrokersDown", KafkaResponseError RdKafkaRespErrAllBrokersDown),
    ("KafkaBadConfiguration", KafkaBadConfiguration),
    ("KafkaBadSpecification", KafkaBadSpecification ""),
    -- A commit-only condition must not be mistaken for a benign poll
    -- condition; the two tables are independent.
    ("RdKafkaRespErrNoOffset", KafkaResponseError RdKafkaRespErrNoOffset)
  ]

tests :: TestTree
tests =
  testGroup
    "Classify"
    [ testGroup "classifyPollError" classifyPollErrorTests,
      testGroup "isBenignCommitError" isBenignCommitErrorTests
    ]

classifyPollErrorTests :: [TestTree]
classifyPollErrorTests =
  [ testCase "timeout: RdKafkaRespErrTimedOut" $
      classifyPollError (KafkaResponseError RdKafkaRespErrTimedOut) @?= PollTimeout
  ]
    <> [ testCase ("benign: " <> label) (classifyPollError err @?= PollBenign)
       | (label, err) <- benignPollErrors
       ]
    <> [ testCase ("throws: " <> label) (classifyPollError err @?= PollThrow)
       | (label, err) <- throwingPollErrors
       ]

isBenignCommitErrorTests :: [TestTree]
isBenignCommitErrorTests =
  [ testCase "accepts RdKafkaRespErrNoOffset" $
      isBenignCommitError (KafkaResponseError RdKafkaRespErrNoOffset) @?= True,
    testCase "rejects RdKafkaRespErrAllBrokersDown" $
      isBenignCommitError (KafkaResponseError RdKafkaRespErrAllBrokersDown) @?= False,
    testCase "rejects RdKafkaRespErrFatal" $
      isBenignCommitError (KafkaResponseError RdKafkaRespErrFatal) @?= False,
    testCase "rejects a partition-EOF poll condition" $
      isBenignCommitError (KafkaResponseError RdKafkaRespErrPartitionEof) @?= False
  ]
