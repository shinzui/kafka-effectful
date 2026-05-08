let Schema =
      https://raw.githubusercontent.com/shinzui/mori-schema/1f70781427426c09673d46f8e6733b7e7d0abedc/package.dhall
        sha256:3b79aae9216456678300441ca8616b64a4b4fa520a1286dfcc418f60899d5d4a

let Cookbook =
      https://raw.githubusercontent.com/shinzui/mori-schema/1f70781427426c09673d46f8e6733b7e7d0abedc/extensions/cookbook/package.dhall
        sha256:5d41094fcc37d35ddef48af2e0401764d0ae77f9bd25127a979473b964affbb7

in  Cookbook.CookbookCatalog::{ entries =
      [ Cookbook.CookbookEntry::{ key = "getting-started"
        , title = "Get started with kafka-effectful — producer, consumer, and tracing"
        , contentType = Cookbook.ContentType.Instructions
        , topics =
          [ Cookbook.Topic.Streaming
          , Cookbook.Topic.Effects
          , Cookbook.Topic.Observability
          ]
        , packages = [ "kafka-effectful", "hw-kafka-client", "effectful" ]
        , language = Schema.Language.Haskell
        , audience = Schema.DocAudience.User
        , location = Schema.DocLocation.LocalFile "README.md"
        , description = Some
            "Overview, the eight producer scenarios mirroring hw-kafka-client's producer-best-practices guide, the consumer poll/commit loop, runEff/runError wiring, and OpenTelemetry tracing — all in one place"
        }
      , Cookbook.CookbookEntry::{ key = "sync-publish"
        , title = "Publish a record synchronously and capture the broker offset"
        , contentType = Cookbook.ContentType.SampleCode
        , topics = [ Cookbook.Topic.Streaming ]
        , packages = [ "kafka-effectful", "hw-kafka-client", "effectful" ]
        , language = Schema.Language.Haskell
        , audience = Schema.DocAudience.User
        , location = Schema.DocLocation.LocalFile "examples/SyncPublish.hs"
        , description = Some
            "Scenario 2 of producer-best-practices — produceMessageSync blocks until the broker assigns an offset; returns Offset on success and throws KafkaError on failure"
        }
      , Cookbook.CookbookEntry::{ key = "transactional-etl"
        , title = "Run a transactional consume-transform-produce loop"
        , contentType = Cookbook.ContentType.SampleCode
        , topics =
          [ Cookbook.Topic.Streaming, Cookbook.Topic.ErrorHandling ]
        , packages = [ "kafka-effectful", "hw-kafka-client", "effectful" ]
        , language = Schema.Language.Haskell
        , audience = Schema.DocAudience.User
        , location = Schema.DocLocation.LocalFile "examples/TransactionalEtl.hs"
        , description = Some
            "Scenario 5 of producer-best-practices — exactly-once ETL: consume, transform, produce, and commit consumer offsets inside one producer transaction with TxError dispatch (abort/retry/fatal)"
        }
      , Cookbook.CookbookEntry::{ key = "otel-tracing"
        , title = "Propagate OpenTelemetry traces through Kafka headers"
        , contentType = Cookbook.ContentType.SampleCode
        , topics =
          [ Cookbook.Topic.Observability, Cookbook.Topic.Streaming ]
        , packages =
          [ "kafka-effectful", "hs-opentelemetry-api", "hw-kafka-client" ]
        , language = Schema.Language.Haskell
        , audience = Schema.DocAudience.User
        , location = Schema.DocLocation.LocalFile "examples/OtelTracing.hs"
        , description = Some
            "End-to-end demo of runKafkaProducerTraced and runKafkaConsumerTraced — the W3C traceparent injected into outgoing headers is extracted by the consumer interpreter so producer and consumer share one trace ID"
        }
      ]
    }
