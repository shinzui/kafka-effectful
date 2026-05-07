let Schema =
      https://raw.githubusercontent.com/shinzui/mori-schema/1f70781427426c09673d46f8e6733b7e7d0abedc/package.dhall
        sha256:3b79aae9216456678300441ca8616b64a4b4fa520a1286dfcc418f60899d5d4a

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
