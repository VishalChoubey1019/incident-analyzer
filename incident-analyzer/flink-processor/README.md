# flink-processor

Kotlin/Flink job that reads from `logs` and `alerts` Kafka topics,
correlates events per service in 30-second windows, and publishes
correlated incidents to the `incidents` topic.

## Build

Requires Java 17+ and Gradle (or use the wrapper).

```bash
# build fat jar
./gradlew shadowJar

# output: build/libs/flink-processor-0.1.0.jar
```

## Submit to local Flink cluster

```bash
# via Flink REST API
curl -X POST http://localhost:8081/jars/upload \
  -H "Expect:" \
  -F "jarfile=@build/libs/flink-processor-0.1.0.jar"

# then start the job via the Flink UI at http://localhost:8081
# or use the CLI if you have it:
# flink run build/libs/flink-processor-0.1.0.jar
```

## Logic

- **Log stream**: groups ERROR-level log events by service in 30-second
  tumbling windows. If ≥3 errors arrive in a window, it emits a
  correlated incident.
- **Alert stream**: every incoming alert becomes an incident immediately
  (no windowing).
- **Severity heuristic**: based on error count in the window
  (≥10 → CRITICAL, ≥6 → HIGH, ≥3 → MEDIUM).

## Running locally (without Flink cluster)

For development you can run the job directly via Gradle:

```bash
./gradlew run
```

This uses Flink's local mini-cluster mode — no Docker needed.
