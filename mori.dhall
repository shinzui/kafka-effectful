let Schema =
      https://raw.githubusercontent.com/shinzui/mori-schema/ad9960dd3dd3b33eadd45f17bcf430b0e1ec13bc/package.dhall
        sha256:83aa1432e98db5da81afde4ab2057dcab7ce4b2e883d0bc7f16c7d25b917dd0c

in  Schema.Project::{ project =
      Schema.ProjectIdentity::{ name = "kafka-effectful"
      , namespace = "shinzui"
      , type = Schema.PackageType.Library
      , description = Some
          "Effectful effects and interpreters for hw-kafka-client, providing typed composable KafkaProducer and KafkaConsumer effects"
      , language = Schema.Language.Haskell
      , lifecycle = Schema.Lifecycle.Experimental
      , domains = [ "Kafka", "Messaging" ]
      , owners = [ "Nadeem Bitar" ]
      }
    , repos =
      [ Schema.Repo::{ name = "kafka-effectful"
        , github = Some "shinzui/kafka-effectful"
        }
      ]
    , packages =
      [ Schema.Package::{ name = "kafka-effectful"
        , type = Schema.PackageType.Library
        , language = Schema.Language.Haskell
        , description = Some
            "Effectful effects and interpreters for hw-kafka-client"
        }
      ]
    , dependencies =
      [ "effectful"
      , "hw-kafka-client"
      , "librdkafka"
      ]
    }
