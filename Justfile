# Justfile for kafka-effectful

default:
    @just --list


# --- Services ---

# Start Redpanda and Jaeger via process-compose. Runs in the foreground.
[group("services")]
process-up:
    process-compose --tui=false --unix-socket .dev/process-compose.sock up

# Stop Redpanda and Jaeger.
[group("services")]
process-down:
    process-compose --unix-socket .dev/process-compose.sock down || true

# Open the Jaeger UI in the default browser.
[group("services")]
jaeger-ui:
    open http://localhost:16686

# Tail Jaeger logs.
[group("services")]
jaeger-logs:
    process-compose --unix-socket .dev/process-compose.sock process logs jaeger -f


# --- Kafka ---

# Create topics used by examples.
[group("kafka")]
create-topics:
    rpk topic create kafka-effectful-sync-demo -p 1 || true
    rpk topic create source -p 1 || true
    rpk topic create destination -p 1 || true
    rpk topic create otel-demo -p 1 || true

# Delete topics used by examples.
[group("kafka")]
delete-topics:
    rpk topic delete kafka-effectful-sync-demo source destination otel-demo || true

# List all topics.
[group("kafka")]
list-topics:
    rpk topic list


# --- Build and test ---

# Build all enabled components.
[group("build")]
build:
    cabal build all

# Run the test suite.
[group("build")]
test:
    cabal test kafka-effectful-test

# Build examples.
[group("build")]
build-examples:
    cabal build -f examples all

# Run the OpenTelemetry tracing example against local Redpanda.
[group("build")]
otel-example:
    cabal run example-otel-tracing -f examples -- --bootstrap-servers localhost:9092 --topic otel-demo

# Format code via treefmt-nix.
[group("build")]
fmt:
    nix fmt
