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

## Module Structure

| Module | Description |
|--------|-------------|
| `Kafka.Effectful` | Convenience re-export of both effects and common types |
| `Kafka.Effectful.Producer` | Producer effect, interpreter, and types |
| `Kafka.Effectful.Consumer` | Consumer effect, interpreter, and types |
| `Kafka.Effectful.Producer.Effect` | `KafkaProducer` effect definition and operations |
| `Kafka.Effectful.Producer.Interpreter` | `runKafkaProducer` interpreter |
| `Kafka.Effectful.Consumer.Effect` | `KafkaConsumer` effect definition and operations |
| `Kafka.Effectful.Consumer.Interpreter` | `runKafkaConsumer` interpreter |

## Requirements

- GHC >= 9.12
- librdkafka (system dependency)

## License

MIT
