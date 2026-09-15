let Schema =
      https://raw.githubusercontent.com/shinzui/mori-schema/3522f4a51181d73c9c90fc27a7c0838bd29ae95f/package.dhall
        sha256:dcb19e2312e790bad14e622cc98a1281cd2298c5b564a2f0d0534d3c718d8803

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
        , dependencies =
          [ Schema.Dependency.WithAugmentation
              { name = "effectful/effectful:effectful-core"
              , extraDocs = [] : List Schema.DocRef.Type
              , localPathOverride = None Text
              , kind = Some Schema.DependencyKind.ThirdParty
              , source = Some Schema.DependencySource.Hackage
              , scope = Some Schema.DependencyScope.Regular
              , versionConstraint = Some ">=2.5 && <2.8"
              }
          , Schema.Dependency.WithAugmentation
              { name = "shinzui/hw-kafka-client"
              , extraDocs = [] : List Schema.DocRef.Type
              , localPathOverride = None Text
              , kind = Some Schema.DependencyKind.ThirdParty
              , source = Some Schema.DependencySource.GitHub
              , scope = Some Schema.DependencyScope.Regular
              , versionConstraint = Some ">=5.3 && <6"
              }
          , Schema.Dependency.WithAugmentation
              { name = "iand675/hs-opentelemetry:hs-opentelemetry-api"
              , extraDocs = [] : List Schema.DocRef.Type
              , localPathOverride = None Text
              , kind = Some Schema.DependencyKind.ThirdParty
              , source = Some Schema.DependencySource.Hackage
              , scope = Some Schema.DependencyScope.Regular
              , versionConstraint = Some "^>=1.0"
              }
          , Schema.Dependency.WithAugmentation
              { name =
                  "iand675/hs-opentelemetry:hs-opentelemetry-semantic-conventions"
              , extraDocs = [] : List Schema.DocRef.Type
              , localPathOverride = None Text
              , kind = Some Schema.DependencyKind.ThirdParty
              , source = Some Schema.DependencySource.Hackage
              , scope = Some Schema.DependencyScope.Regular
              , versionConstraint = Some ">=1.40 && <2"
              }
          , Schema.Dependency.WithAugmentation
              { name = "iand675/hs-opentelemetry:hs-opentelemetry-sdk"
              , extraDocs = [] : List Schema.DocRef.Type
              , localPathOverride = None Text
              , kind = Some Schema.DependencyKind.ThirdParty
              , source = Some Schema.DependencySource.Hackage
              , scope = Some Schema.DependencyScope.Test
              , versionConstraint = Some "^>=1.0"
              }
          , Schema.Dependency.WithAugmentation
              { name =
                  "iand675/hs-opentelemetry:hs-opentelemetry-exporter-in-memory"
              , extraDocs = [] : List Schema.DocRef.Type
              , localPathOverride = None Text
              , kind = Some Schema.DependencyKind.ThirdParty
              , source = Some Schema.DependencySource.Hackage
              , scope = Some Schema.DependencyScope.Test
              , versionConstraint = Some "^>=1.0"
              }
          , Schema.Dependency.WithAugmentation
              { name = "iand675/hs-opentelemetry:hs-opentelemetry-exporter-otlp"
              , extraDocs = [] : List Schema.DocRef.Type
              , localPathOverride = None Text
              , kind = Some Schema.DependencyKind.ThirdParty
              , source = Some Schema.DependencySource.Hackage
              , scope = Some Schema.DependencyScope.Dev
              , versionConstraint = Some "^>=1.0"
              }
          , Schema.Dependency.WithAugmentation
              { name = "UnkindPartition/tasty:tasty"
              , extraDocs = [] : List Schema.DocRef.Type
              , localPathOverride = None Text
              , kind = Some Schema.DependencyKind.ThirdParty
              , source = Some Schema.DependencySource.Hackage
              , scope = Some Schema.DependencyScope.Test
              , versionConstraint = Some "^>=1.5"
              }
          , Schema.Dependency.WithAugmentation
              { name = "UnkindPartition/tasty:tasty-hunit"
              , extraDocs = [] : List Schema.DocRef.Type
              , localPathOverride = None Text
              , kind = Some Schema.DependencyKind.ThirdParty
              , source = Some Schema.DependencySource.Hackage
              , scope = Some Schema.DependencyScope.Test
              , versionConstraint = Some "^>=0.10"
              }
          ]
        }
      ]
    , dependencies =
      [ "effectful/effectful:effectful-core"
      , "shinzui/hw-kafka-client"
      , "confluentinc/librdkafka"
      , "iand675/hs-opentelemetry:hs-opentelemetry-api"
      , "iand675/hs-opentelemetry:hs-opentelemetry-semantic-conventions"
      , "iand675/hs-opentelemetry:hs-opentelemetry-sdk"
      , "iand675/hs-opentelemetry:hs-opentelemetry-exporter-in-memory"
      , "iand675/hs-opentelemetry:hs-opentelemetry-exporter-otlp"
      , "UnkindPartition/tasty:tasty"
      , "UnkindPartition/tasty:tasty-hunit"
      ]
    , dependencyRefs =
      [ Schema.MoriRef::{ namespace = "effectful"
        , name = "effectful"
        , kind = Some Schema.MoriArtifactKind.Package
        , key = Some "effectful-core"
        }
      , Schema.MoriRef::{ namespace = "shinzui", name = "hw-kafka-client" }
      , Schema.MoriRef::{ namespace = "confluentinc", name = "librdkafka" }
      , Schema.MoriRef::{ namespace = "iand675"
        , name = "hs-opentelemetry"
        , kind = Some Schema.MoriArtifactKind.Package
        , key = Some "hs-opentelemetry-api"
        }
      , Schema.MoriRef::{ namespace = "iand675"
        , name = "hs-opentelemetry"
        , kind = Some Schema.MoriArtifactKind.Package
        , key = Some "hs-opentelemetry-semantic-conventions"
        }
      , Schema.MoriRef::{ namespace = "iand675"
        , name = "hs-opentelemetry"
        , kind = Some Schema.MoriArtifactKind.Package
        , key = Some "hs-opentelemetry-sdk"
        }
      , Schema.MoriRef::{ namespace = "iand675"
        , name = "hs-opentelemetry"
        , kind = Some Schema.MoriArtifactKind.Package
        , key = Some "hs-opentelemetry-exporter-in-memory"
        }
      , Schema.MoriRef::{ namespace = "iand675"
        , name = "hs-opentelemetry"
        , kind = Some Schema.MoriArtifactKind.Package
        , key = Some "hs-opentelemetry-exporter-otlp"
        }
      , Schema.MoriRef::{ namespace = "UnkindPartition"
        , name = "tasty"
        , kind = Some Schema.MoriArtifactKind.Package
        , key = Some "tasty"
        }
      , Schema.MoriRef::{ namespace = "UnkindPartition"
        , name = "tasty"
        , kind = Some Schema.MoriArtifactKind.Package
        , key = Some "tasty-hunit"
        }
      ]
    }
