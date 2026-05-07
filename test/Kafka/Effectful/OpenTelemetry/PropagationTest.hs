module Kafka.Effectful.OpenTelemetry.PropagationTest (tests) where

import Data.ByteString (ByteString)
import Data.ByteString.Char8 qualified as BSC
import Data.CaseInsensitive qualified as CI
import Data.Text (Text)
import Data.Text qualified as Text
import Kafka.Consumer.Types (
    ConsumerRecord (..),
    Offset (..),
    Timestamp (NoTimestamp),
 )
import Kafka.Effectful.OpenTelemetry.Propagation (
    extractTraceContextFromRecord,
    injectTraceContextIntoRecord,
    kafkaHeadersToRequestHeaders,
    requestHeadersToKafkaHeaders,
 )
import Kafka.Producer.Types (
    ProducePartition (UnassignedPartition),
    ProducerRecord (..),
 )
import Kafka.Types (
    PartitionId (..),
    TopicName (..),
    headersFromList,
    headersToList,
 )
import OpenTelemetry.Context qualified as Context
import OpenTelemetry.Trace (initializeGlobalTracerProvider)
import OpenTelemetry.Trace.Core (
    SpanContext (..),
    defaultTraceFlags,
    getSpanContext,
    wrapSpanContext,
 )
import OpenTelemetry.Trace.Id (
    Base (Base16),
    SpanId,
    TraceId,
    baseEncodedToSpanId,
    baseEncodedToTraceId,
    traceIdBaseEncodedText,
 )
import OpenTelemetry.Trace.TraceState qualified as TraceState
import Test.Tasty (TestTree, testGroup, withResource)
import Test.Tasty.HUnit (assertBool, assertEqual, assertFailure, testCase)

{- | Known traceparent value derived from the W3C trace-context
specification\'s example. Trace ID
@0af7651916cd43dd8448eb211c80319c@, span ID
@b7ad6b7169203331@, sampled (flag @01@).
-}
sampleTraceparent :: ByteString
sampleTraceparent =
    "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"

sampleTraceIdHex :: Text
sampleTraceIdHex = "0af7651916cd43dd8448eb211c80319c"

sampleSpanIdHex :: Text
sampleSpanIdHex = "b7ad6b7169203331"

tests :: TestTree
tests =
    -- Initialize the global tracer provider exactly once for the whole
    -- test group. The SDK\'s default propagator pipeline includes
    -- @w3cTraceContextPropagator@, which is what
    -- @injectTraceContextIntoRecord@ \/ @extractTraceContextFromRecord@
    -- consult under the hood.
    withResource initializeGlobalTracerProvider (\_ -> pure ()) $ \_ ->
        testGroup
            "Propagation"
            [ testCase "round-trip kafka headers <-> request headers (case-fold)" $ do
                let original =
                        headersFromList
                            [ ("traceparent", sampleTraceparent)
                            , ("Custom-Header", "value")
                            ]
                    roundTripped =
                        requestHeadersToKafkaHeaders
                            (kafkaHeadersToRequestHeaders original)
                lookup "traceparent" (headersToList roundTripped)
                    `shouldBeJust` sampleTraceparent
                lookup "custom-header" (headersToList roundTripped)
                    `shouldBeJust` "value"
            , testCase "kafkaHeadersToRequestHeaders preserves traceparent value" $ do
                let h = headersFromList [("traceparent", sampleTraceparent)]
                    rh = kafkaHeadersToRequestHeaders h
                lookup (CI.mk "traceparent") rh
                    `shouldBeJust` sampleTraceparent
            , testCase "injectTraceContextIntoRecord adds W3C traceparent" $ do
                tid <- decodeHexTraceId sampleTraceIdHex
                sid <- decodeHexSpanId sampleSpanIdHex
                let ctx =
                        Context.insertSpan
                            (wrapSpanContext (frozenContextWith tid sid))
                            Context.empty
                injected <-
                    injectTraceContextIntoRecord
                        ctx
                        emptyProducerRecord
                let injectedHeaders = headersToList (prHeaders injected)
                case lookup "traceparent" injectedHeaders of
                    Nothing ->
                        assertFailure
                            "expected the producer record to carry a traceparent header"
                    Just header ->
                        assertBool
                            ( "traceparent did not contain the trace-id "
                                <> Text.unpack sampleTraceIdHex
                                <> ", got: "
                                <> BSC.unpack header
                            )
                            (BSC.pack (Text.unpack sampleTraceIdHex) `BSC.isInfixOf` header)
            , testCase "extractTraceContextFromRecord recovers parent context" $ do
                let cr = consumerRecordWithHeaders sampleTraceparent
                ctx <-
                    extractTraceContextFromRecord cr Context.empty
                case Context.lookupSpan ctx of
                    Nothing ->
                        assertFailure
                            "expected the extracted context to carry a span"
                    Just span_ -> do
                        sc <- getSpanContext span_
                        let recovered = traceIdBaseEncodedText Base16 (traceId sc)
                        assertEqual
                            "recovered trace-id should match the inbound traceparent"
                            sampleTraceIdHex
                            recovered
            ]

shouldBeJust :: (Eq a, Show a) => Maybe a -> a -> IO ()
shouldBeJust Nothing expected =
    assertFailure ("expected Just " <> show expected <> ", got Nothing")
shouldBeJust (Just actual) expected =
    assertEqual "values differ" expected actual

decodeHexTraceId :: Text -> IO TraceId
decodeHexTraceId hex =
    case baseEncodedToTraceId Base16 (BSC.pack (Text.unpack hex)) of
        Right tid -> pure tid
        Left err -> assertFailure ("invalid hex trace id: " <> err) >> error "unreachable"

decodeHexSpanId :: Text -> IO SpanId
decodeHexSpanId hex =
    case baseEncodedToSpanId Base16 (BSC.pack (Text.unpack hex)) of
        Right sid -> pure sid
        Left err -> assertFailure ("invalid hex span id: " <> err) >> error "unreachable"

{- | Build a 'SpanContext' that carries the supplied trace and span
IDs. Trace flags are 'defaultTraceFlags' (unsampled); the W3C
propagator preserves the bytes anyway.
-}
frozenContextWith :: TraceId -> SpanId -> SpanContext
frozenContextWith tid sid =
    SpanContext
        { traceId = tid
        , spanId = sid
        , traceFlags = defaultTraceFlags
        , isRemote = False
        , traceState = TraceState.empty
        }

emptyProducerRecord :: ProducerRecord
emptyProducerRecord =
    ProducerRecord
        { prTopic = TopicName "demo"
        , prPartition = UnassignedPartition
        , prKey = Nothing
        , prValue = Just "value"
        , prHeaders = headersFromList []
        }

consumerRecordWithHeaders ::
    ByteString ->
    ConsumerRecord (Maybe ByteString) (Maybe ByteString)
consumerRecordWithHeaders traceparent =
    ConsumerRecord
        { crTopic = TopicName "demo"
        , crPartition = PartitionId 0
        , crOffset = Offset 0
        , crTimestamp = NoTimestamp
        , crHeaders = headersFromList [("traceparent", traceparent)]
        , crKey = Nothing
        , crValue = Just "value"
        }
