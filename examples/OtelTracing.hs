{- | End-to-end OpenTelemetry tracing demo.

This program produces a single record through 'runKafkaProducerTraced'
and then consumes it through 'runKafkaConsumerTraced'. It captures the
producer-side trace ID (from an outer parent span we open so its trace
ID propagates to the produce span the interpreter opens) and the
consumer-side trace ID (from the inbound parent span the consumer
interpreter attaches to the thread context after extracting the W3C
@traceparent@ header), then compares the two. A successful run prints
two equal trace IDs and exits zero. A mismatch (or any kafka error)
exits non-zero.

Manual run procedure:

@
  cabal run example-otel-tracing -f examples -- \\
    --bootstrap-servers localhost:9092 \\
    --topic otel-demo
@

Expected output (trace IDs are 32-hex-char strings, regenerated each
run):

@
  [otel-tracing] producer trace id: 4bf92f3577b34da6a3ce929d0e0e4736
  [otel-tracing] consumer trace id: 4bf92f3577b34da6a3ce929d0e0e4736
  [otel-tracing] trace IDs match — context propagated through Kafka headers
@

If a Jaeger v2 instance is reachable at @http:\/\/localhost:4318@
(the default OTLP HTTP endpoint), the same trace appears with a
producer span as the parent and a child consumer span. Otherwise the
flow still works locally and the OTLP exporter silently drops the
spans.
-}
module Main (main) where

import Data.IORef (IORef, newIORef, readIORef, writeIORef)
import Data.Text (Text)
import Data.Text qualified as Text
import Effectful (Eff, IOE, runEff, (:>))
import Effectful qualified
import Effectful.Error.Static (runError)
import Kafka.Effectful
import Kafka.Effectful.Consumer qualified as KEC
import Kafka.Effectful.OpenTelemetry (
    runKafkaConsumerTraced,
    runKafkaProducerTraced,
 )
import Kafka.Effectful.Producer qualified as KEP
import OpenTelemetry.Attributes (emptyAttributes)
import OpenTelemetry.Context qualified as Context
import OpenTelemetry.Context.ThreadLocal (getContext)
import OpenTelemetry.Trace (
    initializeGlobalTracerProvider,
    makeTracer,
    shutdownTracerProvider,
    tracerOptions,
 )
import OpenTelemetry.Trace.Core (
    InstrumentationLibrary (..),
    SpanContext (traceId),
    Tracer,
    defaultSpanArguments,
    getSpanContext,
    inSpan'',
 )
import OpenTelemetry.Trace.Id (Base (Base16), traceIdBaseEncodedText)
import System.Environment (getArgs)
import System.Exit (exitFailure, exitSuccess)
import System.IO (hPutStrLn, stderr)

-- CLI

data Args = Args
    { bootstrapServers :: Text
    , topic :: Text
    }

parseArgs :: [String] -> Maybe Args
parseArgs = go (Args "" "")
  where
    go acc [] =
        if Text.null acc.bootstrapServers || Text.null acc.topic
            then Nothing
            else Just acc
    go acc ("--bootstrap-servers" : v : rest) =
        go acc{bootstrapServers = Text.pack v} rest
    go acc ("--topic" : v : rest) = go acc{topic = Text.pack v} rest
    go _ _ = Nothing

usage :: String
usage =
    "usage: example-otel-tracing --bootstrap-servers HOST:PORT --topic NAME"

-- Wiring

producerProps :: Text -> ProducerProperties
producerProps host =
    KEP.brokersList [BrokerAddress host]
        <> KEP.sendTimeout (Timeout 10000)
        <> KEP.extraProp "enable.idempotence" "true"
        <> KEP.extraProp "acks" "all"

consumerProps :: Text -> ConsumerProperties
consumerProps host =
    KEC.brokersList [BrokerAddress host]
        <> KEC.groupId (ConsumerGroupId "kafka-effectful-otel-demo")

mkRecord :: Text -> ProducerRecord
mkRecord t =
    ProducerRecord
        { prTopic = TopicName t
        , prPartition = UnassignedPartition
        , prKey = Just "k1"
        , prValue = Just "hello otel"
        , prHeaders = mempty
        }

instrumentation :: InstrumentationLibrary
instrumentation =
    InstrumentationLibrary
        { libraryName = "kafka-effectful-example"
        , libraryVersion = "0.2.0.0"
        , librarySchemaUrl = ""
        , libraryAttributes = emptyAttributes
        }

-- Capture the trace ID of whatever span is currently active on the
-- thread. Used on both sides: the producer wraps its work in an
-- outer 'inSpan''' so a parent span is active; the consumer
-- interpreter attaches the inbound parent span to the thread
-- context after extracting the @traceparent@ header.
captureCurrentTraceId :: (IOE :> es) => IORef (Maybe Text) -> Eff es ()
captureCurrentTraceId ref = Effectful.liftIO $ do
    ctx <- getContext
    case Context.lookupSpan ctx of
        Nothing -> pure ()
        Just s -> do
            sc <- getSpanContext s
            writeIORef ref (Just (traceIdBaseEncodedText Base16 (traceId sc)))

-- Poll up to 30 one-second timeouts looking for the record we just
-- produced. Captures the consumer-side trace ID after the first
-- successful poll: at that point the consumer interpreter has
-- attached the inbound parent span, so the active context's trace
-- ID equals what the producer published on the wire.
pollUntilRecord ::
    (KafkaConsumer :> es, IOE :> es) =>
    IORef (Maybe Text) ->
    Eff es ()
pollUntilRecord ref = loop (30 :: Int)
  where
    loop 0 = pure ()
    loop n = do
        mbRecord <- pollMessage (Timeout 1000)
        case mbRecord of
            Nothing -> loop (n - 1)
            Just _ -> captureCurrentTraceId ref

main :: IO ()
main = do
    rawArgs <- getArgs
    args <- case parseArgs rawArgs of
        Nothing -> hPutStrLn stderr usage *> exitFailure
        Just a -> pure a

    tp <- initializeGlobalTracerProvider
    let tracer :: Tracer
        tracer = makeTracer tp instrumentation tracerOptions

    producerTraceRef <- newIORef Nothing
    consumerTraceRef <- newIORef Nothing

    producerResult <-
        runEff . runError @KafkaError $
            runKafkaProducerTraced tracer (producerProps args.bootstrapServers) $
                inSpan'' tracer "publish" defaultSpanArguments $ \_parentSpan -> do
                    captureCurrentTraceId producerTraceRef
                    produceMessage (mkRecord args.topic)
                    flushProducer
    case producerResult of
        Left (_cs, err) -> do
            hPutStrLn stderr $ "producer error: " <> show err
            _ <- shutdownTracerProvider tp Nothing
            exitFailure
        Right () -> pure ()

    consumerResult <-
        runEff . runError @KafkaError $
            runKafkaConsumerTraced
                tracer
                (consumerProps args.bootstrapServers)
                (topics [TopicName args.topic] <> offsetReset Earliest)
                (pollUntilRecord consumerTraceRef)
    case consumerResult of
        Left (_cs, err) -> do
            hPutStrLn stderr $ "consumer error: " <> show err
            _ <- shutdownTracerProvider tp Nothing
            exitFailure
        Right () -> pure ()

    pid <- readIORef producerTraceRef
    cid <- readIORef consumerTraceRef
    _ <- shutdownTracerProvider tp Nothing
    case (pid, cid) of
        (Just p, Just c) -> do
            putStrLn $ "[otel-tracing] producer trace id: " <> Text.unpack p
            putStrLn $ "[otel-tracing] consumer trace id: " <> Text.unpack c
            if p == c
                then do
                    putStrLn
                        "[otel-tracing] trace IDs match — context propagated through Kafka headers"
                    exitSuccess
                else do
                    putStrLn "[otel-tracing] trace IDs differ"
                    exitFailure
        (Nothing, _) -> do
            hPutStrLn stderr "[otel-tracing] failed to capture producer trace id"
            exitFailure
        (_, Nothing) -> do
            hPutStrLn stderr "[otel-tracing] consumer never received the record (timed out)"
            exitFailure
