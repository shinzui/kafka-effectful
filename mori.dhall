let Schema =
      https://raw.githubusercontent.com/shinzui/mori-schema/026ae74331e5c516542af1dd96f041c658ed4621/package.dhall
        sha256:18258ef583580a897f4af3e7c86db0342afb42fb40efc535b217ba1089230141

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
