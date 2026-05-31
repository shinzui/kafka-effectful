# kafka-effectful

Effectful effects and interpreters for [hw-kafka-client](https://hackage.haskell.org/package/hw-kafka-client), a Haskell binding to Apache Kafka via librdkafka.

Provides typed, composable `KafkaProducer` and `KafkaConsumer` effects for the [effectful](https://hackage.haskell.org/package/effectful) ecosystem.

> **Status: experimental.** This package is on its first release. The API may change in breaking ways in subsequent 0.x versions. Pin to an exact version in production until 1.0 is tagged.

## Features

- **KafkaProducer** -- send messages and flush the producer queue
- **KafkaConsumer** -- poll messages (single or batch), manage offsets, assign/pause/resume/seek partitions, and query committed offsets, positions, assignments, and subscriptions
- Resource-safe interpreters that acquire and release Kafka handles via `bracket`
- Errors surfaced through `Effectful.Error.Static` (`Error KafkaError`)

## Usage

### Producer

```haskell
import Kafka.Effectful

example :: (IOE :> es, Error KafkaError :> es) => Eff es ()
example =
  runKafkaProducer producerProps $ do
    produceMessage record
    flushProducer
```

### Producer scenarios

The eight scenarios below mirror the
[producer-best-practices](https://github.com/haskell-works/hw-kafka-client/blob/master/docs/producer-best-practices.md)
guide from `hw-kafka-client`. Each snippet uses only symbols exported
from `Kafka.Effectful`; when a snippet needs `produceMessage'` or
`askProducerHandle`, it also imports `Kafka.Effectful.Producer`.

##### Scenario 1 — Fire-and-forget

Register a global delivery callback via `ProducerProperties` and
enqueue without blocking. `runKafkaProducer` flushes on scope exit.

```haskell
fireAndForgetProps =
  brokersList ["localhost:9092"]
  <> setCallback (deliveryCallback logFailure)

runKafkaProducer fireAndForgetProps $
  forM_ events (produceMessage . toRecord)
```

##### Scenario 2 — Synchronous delivery confirmation

`produceMessageSync` allocates an `MVar`, flushes the producer, and
returns the broker-assigned `Offset` — throwing `KafkaError` on any
failure.

```haskell
runKafkaProducer producerProps $ do
  offset <- produceMessageSync record
  liftIO $ putStrLn ("stored at offset " <> show offset)
```

##### Scenario 3 — Idempotent producer

Configuration only — no new call site is needed. Safe to enable by
default; the broker deduplicates retries by `(producer-id, sequence)`.

```haskell
idempotentProps =
  brokersList ["localhost:9092"]
  <> extraProp "enable.idempotence" "true"
  <> extraProp "acks" "all"
  <> extraProp "max.in.flight.requests.per.connection" "5"
```

##### Scenario 4 — High-throughput batching

Combine `produceMessageBatch` with `linger.ms`, `batch.size`, and
`compression` to trade a few milliseconds of latency for substantially
higher throughput. The result contains only records that failed to
enqueue.

```haskell
batchProps =
  brokersList ["localhost:9092"]
  <> compression Snappy
  <> extraProp "linger.ms" "10"
  <> extraProp "batch.size" "65536"

runKafkaProducer batchProps $ do
  failures <- produceMessageBatch records
  unless (null failures) $
    liftIO $ putStrLn ("enqueue failures: " <> show (length failures))
```

##### Scenario 5 — Transactional ETL

Consume, transform, produce, and commit consumer offsets — all inside
one producer transaction. `commitOffsetMessageTransaction` requires
both the `KafkaProducer` and `KafkaConsumer` effects. `TxError` must
be dispatched on in a fixed order: `kafkaErrorTxnRequiresAbort`,
`kafkaErrorIsRetriable`, `kafkaErrorIsFatal`.

```haskell
txProps =
  brokersList ["localhost:9092"]
  <> extraProp "transactional.id" "etl-1"
  <> extraProp "enable.idempotence" "true"
  <> extraProp "acks" "all"

etl = runKafkaProducer txProps $ runKafkaConsumer consumerProps sub $ do
  initTransactions (Timeout 10000)
  forever $ do
    msgs <- pollMessageBatch (Timeout 500) (BatchSize 100)
    let records = rights msgs
    unless (null records) $ do
      beginTransaction
      forM_ records (produceMessage . transform)
      forM_ (lastPerPartition records) $ \r ->
        commitOffsetMessageTransaction r (Timeout 5000)
          >>= handleTxResult
      commitTransaction (Timeout 5000) >>= handleTxResult

handleTxResult Nothing  = pure ()
handleTxResult (Just e)
  | kafkaErrorTxnRequiresAbort e = abortTransaction (Timeout 5000)
  | kafkaErrorIsRetriable e      = liftIO $ putStrLn "retry"
  | kafkaErrorIsFatal e          = throwError (getKafkaError e)
  | otherwise                    = liftIO $ putStrLn (show (getKafkaError e))
```

##### Scenario 6 — Keyed partitioning for ordering

Set `prKey` and leave `prPartition = UnassignedPartition` so the
default hash partitioner routes every record for the same key to the
same partition. Enable idempotence alongside, so a retry does not
reorder.

```haskell
orderedByKey userId event = ProducerRecord
  { prTopic     = TopicName "user-events"
  , prPartition = UnassignedPartition
  , prKey       = Just userId
  , prValue     = Just (encode event)
  , prHeaders   = mempty
  }
```

##### Scenario 7 — Custom partitioning and headers

Target a specific partition with `SpecifiedPartition` and attach
per-message metadata via `headersFromList`.

```haskell
shardedRecord (Shard n) payload = ProducerRecord
  { prTopic     = TopicName "sharded-events"
  , prPartition = SpecifiedPartition n
  , prKey       = Nothing
  , prValue     = Just payload
  , prHeaders   = headersFromList
      [ ("schema-version", "v3")
      , ("source",         "billing-api")
      ]
  }
```

##### Scenario 8 — Graceful shutdown

`runKafkaProducer` already brackets the handle — it flushes and closes
the producer on normal scope exit, so enqueued records drain before
the program continues. No explicit cleanup code is needed.

```haskell
main =
  runEff . runError @KafkaError $
    runKafkaProducer props $ do
      forM_ events (produceMessage . toRecord)
      -- no explicit flush needed; runKafkaProducer flushes on exit
```

### Consumer

```haskell
import Kafka.Effectful

example :: (IOE :> es, Error KafkaError :> es) => Eff es ()
example =
  runKafkaConsumer consumerProps subscription loop
  where
    loop = do
      mbMsg <- pollMessage (Timeout 1000)
      case mbMsg of
        Nothing  -> loop
        Just msg -> do
          commitOffsetMessage OffsetCommit msg
          loop
```

`pollMessage` returns `Nothing` when the timeout elapses without a message
arriving; non-timeout failures are thrown via the `Error KafkaError` effect.

### Running it

The effect handlers `runKafkaProducer` and `runKafkaConsumer` require `IOE` and
`Error KafkaError` in the effect stack. A complete program wires them with
`runEff` and `runError`:

```haskell
{-# LANGUAGE TypeApplications #-}

import Effectful
import Effectful.Error.Static (runError)
import Kafka.Effectful

main :: IO ()
main = do
  result <- runEff . runError @KafkaError $ runProgram
  case result of
    Left (_, err) -> putStrLn ("Kafka error: " <> show err)
    Right ()      -> pure ()
  where
    runProgram =
      runKafkaProducer producerProps $ do
        produceMessage record
        flushProducer
```

Replace `producerProps` and `record` with your own `ProducerProperties` and
`ProducerRecord` values (see the `Kafka.Effectful.Producer` module for the
available builders).

### OpenTelemetry tracing

Swap `runKafkaProducer` for `runKafkaProducerTraced tracer` and
`runKafkaConsumer` for `runKafkaConsumerTraced tracer` to add
distributed tracing without changing any effect-level code. The
traced interpreters open a `Producer`-kind span around every
record-sending operation and a `Consumer`-kind span around every
successful `pollMessage` / per-record success of `pollMessageBatch`,
populated with the OpenTelemetry messaging semantic conventions.
By default the attributes use the legacy-compatible names emitted by
`hs-opentelemetry`'s Kafka instrumentation (`messaging.system`,
`messaging.destination.name`, `messaging.operation`,
`messaging.kafka.destination.partition`,
`messaging.kafka.message.offset`, `messaging.kafka.message.key`,
`messaging.kafka.consumer.group`). Set
`OTEL_SEMCONV_STABILITY_OPT_IN=messaging` to emit the stable v1.40
operation and consumer-group names (`messaging.operation.name`,
`messaging.operation.type`, `messaging.consumer.group.name`) instead,
or `OTEL_SEMCONV_STABILITY_OPT_IN=messaging/dup` to emit both old and
stable names while migrating dashboards. The current OTel context is
injected into the outgoing record's headers as W3C `traceparent` /
`tracestate` so downstream consumers can extract it and continue the
trace.

```haskell
{-# LANGUAGE TypeApplications #-}

import Effectful
import Effectful.Error.Static (runError)
import Kafka.Effectful
import Kafka.Effectful.OpenTelemetry (runKafkaProducerTraced)
import OpenTelemetry.Trace
  ( initializeGlobalTracerProvider
  , makeTracer
  , tracerOptions
  )

main :: IO ()
main = do
  tp <- initializeGlobalTracerProvider
  let tracer = makeTracer tp "my-app" tracerOptions
  result <- runEff . runError @KafkaError $
    runKafkaProducerTraced tracer producerProps $ do
      produceMessage record
      flushProducer
  case result of
    Left (_, err) -> putStrLn ("Kafka error: " <> show err)
    Right ()      -> pure ()
```

Span names are `"send <topic>"` and `"process <topic>"`. The default
interpreters (`runKafkaProducer`, `runKafkaConsumer`) are unchanged
and zero-cost for users who do not want tracing.

**Compatibility with `shibuya-kafka-adapter`.** The attribute keys
this library emits (`messaging.system`,
`messaging.kafka.destination.partition`,
`messaging.kafka.message.offset`) agree with what
`shibuya-kafka-adapter`'s envelope-level attributes already produce.
Layering the two yields a Receive→Process span split:
`kafka-effectful`'s poll span as parent, Shibuya's framework
per-message span as child. If you want only one span per message
instead of two, use either `kafka-effectful`'s traced runner *or*
Shibuya's framework span — not both.

The end-to-end demo in
`examples/OtelTracing.hs` produces a record through the traced
producer and reads it back through the traced consumer, printing
both trace IDs and asserting they match. Run it against a local
broker:

```
cabal run example-otel-tracing -f examples -- \
  --bootstrap-servers localhost:9092 \
  --topic otel-demo
```

## Module Structure

| Module | Description |
|--------|-------------|
| `Kafka.Effectful` | Convenience re-export of both effects and common types |
| `Kafka.Effectful.Producer` | Producer effect, interpreter, and types |
| `Kafka.Effectful.Consumer` | Consumer effect, interpreter, and types |
| `Kafka.Effectful.Producer.Effect` | `KafkaProducer` effect definition and operations |
| `Kafka.Effectful.Producer.Interpreter` | `runKafkaProducer` interpreter |
| `Kafka.Effectful.Producer.Transaction` | Cross-effect `commitOffsetMessageTransaction` helper |
| `Kafka.Effectful.Consumer.Effect` | `KafkaConsumer` effect definition and operations |
| `Kafka.Effectful.Consumer.Interpreter` | `runKafkaConsumer` interpreter |
| `Kafka.Effectful.OpenTelemetry` | Single-import facade for the traced interpreters and helpers |
| `Kafka.Effectful.OpenTelemetry.Producer.Interpreter` | `runKafkaProducerTraced` |
| `Kafka.Effectful.OpenTelemetry.Consumer.Interpreter` | `runKafkaConsumerTraced` |
| `Kafka.Effectful.OpenTelemetry.Semantic` | Pure attribute-builder helpers |
| `Kafka.Effectful.OpenTelemetry.Propagation` | W3C trace-context header bridges |

## Requirements

- GHC >= 9.12
- librdkafka (system dependency)

## License

MIT
