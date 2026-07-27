let UpstreamIssues =
      https://raw.githubusercontent.com/shinzui/mori-schema/93104153ecf8817547229a867302a70a25c4b3d8/extensions/upstream-issues/package.dhall
        sha256:50f8b061a1bd999aac83e3f0ed69cd5a829d2bcf101f556a733f52ed9671e064

in  UpstreamIssues.UpstreamIssuesCatalog::{
    , entries =
      [ UpstreamIssues.UpstreamIssue::{
        , key = "hw-kafka-client-no-produce-batch-binding"
        , dependency = "hw-kafka-client"
        , summary =
            "No binding exists for librdkafka's rd_kafka_produce_batch, so every batch produce is a per-record loop over produceMessage and saves no network round-trips"
        , status = UpstreamIssues.IssueStatus.Workaround
        , revisitTrigger = Some
            "When hw-kafka-client binds rd_kafka_produce_batch. No upstream ticket is filed. Note that upstream's Haskell-level produceMessageBatch, removed in 72e6f6d (Oct 2021, before v5.3.0), was itself a mapM over produceMessage, so restoring that name would not help -- only a real rd_kafka_produce_batch binding would."
        , workaroundPath = Some "src/Kafka/Effectful/Producer/Interpreter.hs"
        , upstreamUrl = Some "https://github.com/haskell-works/hw-kafka-client"
        , tags = [ "kafka", "producer", "batching", "no-upstream-ticket" ]
        }
      ]
    }
