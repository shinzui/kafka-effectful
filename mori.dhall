let Schema =
      https://raw.githubusercontent.com/shinzui/mori-schema/8415b4b8a746a84eecf982f0f1d7194368bf7b54/package.dhall
        sha256:d19ae156d6c357d982a1aea0f1b6ba1f01d76d2d848545b150db75ed4c39a8a9

in  { project =
      { name = "kafka-effectful"
      , namespace = "shinzui"
      , type = Schema.PackageType.Library
      , description = Some
          "Effectful effects and interpreters for hw-kafka-client, providing typed composable KafkaProducer and KafkaConsumer effects"
      , language = Schema.Language.Haskell
      , lifecycle = Schema.Lifecycle.Experimental
      , domains = [ "Kafka", "Messaging" ]
      , owners = [ "Nadeem Bitar" ]
      , origin = Schema.Origin.Own
      }
    , repos =
      [ { name = "kafka-effectful"
        , github = Some "shinzui/kafka-effectful"
        , gitlab = None Text
        , git = None Text
        , localPath = None Text
        }
      ]
    , packages =
      [ { name = "kafka-effectful"
        , type = Schema.PackageType.Library
        , language = Schema.Language.Haskell
        , path = None Text
        , description = Some
            "Effectful effects and interpreters for hw-kafka-client"
        , lifecycle = None Schema.Lifecycle
        , visibility = Schema.Visibility.Public
        , runtime = { deployable = False, exposesApi = False }
        , runtimeEnvironment = None Schema.RuntimeEnvironment
        , dependencies = [] : List Schema.Dependency
        , docs = [] : List Schema.DocRef
        , config = [] : List Schema.ConfigItem
        , apiSource = None Schema.ApiSource
        }
      ]
    , bundles = [] : List Schema.PackageBundle
    , dependencies =
      [ "effectful"
      , "hw-kafka-client"
      , "librdkafka"
      ]
    , apis = [] : List Schema.Api
    , agents = [] : List Schema.AgentHint
    , skills = [] : List Schema.Skill
    , subagents = [] : List Schema.Subagent
    , standards = [] : List Text
    , docs = [] : List Schema.DocRef
    }
