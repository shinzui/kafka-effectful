{- | Trace-context hygiene tests for the traced consumer interpreter.

These drive 'withConsumerSpan' directly rather than going through the
interpreter, because the interpreter closes over a live
@Kafka.Consumer.KafkaConsumer@ and calls hw-kafka-client IO, while the
behaviour under test — how a record\'s inbound trace context is extracted,
attached, and detached — is entirely broker-independent.

Spans are captured with an in-memory exporter wired into a tracer provider
built fresh for each test case. Building it per case rather than sharing one
matters twice over: tasty runs test cases concurrently, so a shared span
reference would race, and @inSpan\'\'@ takes a fast path that skips context
modification entirely when the provider has no span processors, so a
processor-less provider would make these assertions vacuous.
-}
module Kafka.Effectful.OpenTelemetry.ConsumerSpanTest (tests) where

import Data.ByteString (ByteString)
import Data.IORef (readIORef)
import Data.Text (Text)
import Effectful (runEff)
import Kafka.Consumer.ConsumerProperties (ConsumerProperties)
import Kafka.Consumer.Types (
    ConsumerRecord (..),
    Offset (..),
    Timestamp (NoTimestamp),
 )
import Kafka.Effectful.OpenTelemetry.Consumer.Interpreter (withConsumerSpan)
import Kafka.Types (
    PartitionId (..),
    TopicName (..),
    headersFromList,
 )
import OpenTelemetry.Context qualified as Context
import OpenTelemetry.Context.ThreadLocal (getContext)
import OpenTelemetry.Exporter.InMemory.Span (inMemoryListExporter)
import OpenTelemetry.Trace (initializeGlobalTracerProvider)
import OpenTelemetry.Trace.Core (
    ImmutableSpan (..),
    SpanContext (..),
    createTracerProvider,
    emptyTracerProviderOptions,
    forceFlushTracerProvider,
    makeTracer,
    tracerOptions,
 )
import OpenTelemetry.Trace.Id (Base (Base16), traceIdBaseEncodedText)
import Test.Tasty (TestTree, testGroup, withResource)
import Test.Tasty.HUnit (assertBool, assertEqual, assertFailure, testCase)

{- | The W3C trace-context specification\'s example traceparent. Trace ID
@0af7651916cd43dd8448eb211c80319c@.
-}
sampleTraceparent :: ByteString
sampleTraceparent =
    "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"

sampleTraceIdHex :: Text
sampleTraceIdHex = "0af7651916cd43dd8448eb211c80319c"

-- | A second, distinct traceparent, for the batch-isolation case.
otherTraceparent :: ByteString
otherTraceparent =
    "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"

otherTraceIdHex :: Text
otherTraceIdHex = "4bf92f3577b34da6a3ce929d0e0e4736"

{- | An invalid UTF-8 byte sequence: @0xC3@ opens a two-byte sequence but
@0x28@ is not a valid continuation byte.
-}
invalidUtf8 :: ByteString
invalidUtf8 = "\xc3\x28"

mkRecord ::
    [(ByteString, ByteString)] ->
    ConsumerRecord (Maybe ByteString) (Maybe ByteString)
mkRecord headers =
    ConsumerRecord
        { crTopic = TopicName "demo"
        , crPartition = PartitionId 0
        , crOffset = Offset 0
        , crTimestamp = NoTimestamp
        , crHeaders = headersFromList headers
        , crKey = Nothing
        , crValue = Just "value"
        }

emptyProps :: ConsumerProperties
emptyProps = mempty

{- | Run 'withConsumerSpan' over each record in order against a private
tracer provider, and return the exported spans in creation order.
-}
runRecords ::
    [ConsumerRecord (Maybe ByteString) (Maybe ByteString)] ->
    IO [ImmutableSpan]
runRecords records = do
    (processor, spansRef) <- inMemoryListExporter
    provider <- createTracerProvider [processor] emptyTracerProviderOptions
    let tracer = makeTracer provider "kafka-effectful-test" tracerOptions
    runEff $
        mapM_ (\cr -> withConsumerSpan tracer emptyProps cr (pure ())) records
    _ <- forceFlushTracerProvider provider Nothing
    -- inMemoryListExporter conses each span as it ends, so the list is
    -- newest-first; reverse to recover creation order.
    reverse <$> readIORef spansRef

traceIdHexOf :: ImmutableSpan -> Text
traceIdHexOf = traceIdBaseEncodedText Base16 . traceId . spanContext

{- | Whether a context carries a span at all. Enough to detect a leak, and
avoids needing an 'Eq' instance for 'Context'.
-}
hasSpan :: Context.Context -> Bool
hasSpan = maybe False (const True) . Context.lookupSpan

tests :: TestTree
tests =
    -- Establishes the SDK default propagator stack (W3C trace context),
    -- which extractTraceContextFromRecord consults via the global lookup.
    withResource initializeGlobalTracerProvider (\_ -> pure ()) $ \_ ->
        testGroup
            "ConsumerSpan"
            [ testCase "a record with a traceparent gets the remote trace id" $ do
                spans_ <- runRecords [mkRecord [("traceparent", sampleTraceparent)]]
                case spans_ of
                    [s] ->
                        assertEqual
                            "span should inherit the inbound trace id"
                            sampleTraceIdHex
                            (traceIdHexOf s)
                    _ -> assertFailure ("expected exactly one span, got " <> show (length spans_))
            , -- The KSC-6 regression. Before the fix, withConsumerSpan extracted
              -- into the *current* thread-local context and discarded the token
              -- it attached, so the previous record's remote context was still
              -- installed when this headerless record arrived and its span
              -- chained onto it -- contradicting the module's documented "new
              -- root span when no inbound context is present".
              testCase "a headerless record after a traced record starts a new root" $ do
                spans_ <-
                    runRecords
                        [ mkRecord [("traceparent", sampleTraceparent)]
                        , mkRecord []
                        ]
                case spans_ of
                    [_traced, headerless] -> do
                        assertBool
                            ( "headerless record must not inherit the previous record's trace id, got "
                                <> show (traceIdHexOf headerless)
                            )
                            (traceIdHexOf headerless /= sampleTraceIdHex)
                        assertBool
                            "a new root span has no parent"
                            (maybe True (const False) (spanParent headerless))
                    _ -> assertFailure ("expected exactly two spans, got " <> show (length spans_))
            , -- The context leak observed from outside: whatever ambient context
              -- the caller had must survive the call. Before the fix the
              -- thread-local permanently retained the last record's context.
              testCase "the caller's ambient context is restored" $ do
                ctxBefore <- getContext
                _ <-
                    runRecords
                        [ mkRecord [("traceparent", sampleTraceparent)]
                        , mkRecord []
                        ]
                ctxAfter <- getContext
                assertEqual
                    "thread-local context must not retain the last record's context"
                    (hasSpan ctxBefore)
                    (hasSpan ctxAfter)
            , -- The KSC-1 residual. A non-UTF-8 application header must not cost
              -- the record its inbound trace context. Before the fix, building
              -- the carrier decoded every header with partial decodeUtf8, the
              -- exception escaped into the propagator's catch-all, and the whole
              -- context -- traceparent included -- was dropped.
              testCase "a non-UTF-8 header does not poison extraction" $ do
                spans_ <-
                    runRecords
                        [ mkRecord
                            [ ("traceparent", sampleTraceparent)
                            , ("payload-hint", invalidUtf8)
                            ]
                        ]
                case spans_ of
                    [s] ->
                        assertEqual
                            "inbound trace id must survive an undecodable sibling header"
                            sampleTraceIdHex
                            (traceIdHexOf s)
                    _ -> assertFailure ("expected exactly one span, got " <> show (length spans_))
            , -- Pins the PollMessageBatch walk, which calls withConsumerSpan once
              -- per record on a single thread.
              testCase "each record in a batch is isolated from its neighbours" $ do
                spans_ <-
                    runRecords
                        [ mkRecord [("traceparent", sampleTraceparent)]
                        , mkRecord []
                        , mkRecord [("traceparent", otherTraceparent)]
                        ]
                case spans_ of
                    [first_, middle, third] -> do
                        assertEqual
                            "first record keeps its own remote trace"
                            sampleTraceIdHex
                            (traceIdHexOf first_)
                        assertBool
                            "middle record must be a new root, not a continuation"
                            (traceIdHexOf middle /= sampleTraceIdHex)
                        assertEqual
                            "third record picks up its own remote trace"
                            otherTraceIdHex
                            (traceIdHexOf third)
                    _ -> assertFailure ("expected exactly three spans, got " <> show (length spans_))
            ]
