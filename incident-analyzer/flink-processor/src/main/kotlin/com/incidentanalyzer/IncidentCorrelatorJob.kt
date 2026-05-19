package com.incidentanalyzer

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.typeinfo.Types
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema
import org.apache.flink.connector.kafka.sink.KafkaSink
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.windowing.WindowFunction
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import org.slf4j.LoggerFactory

object IncidentCorrelatorJob {

    private val log = LoggerFactory.getLogger(IncidentCorrelatorJob::class.java)

    @JvmStatic
    fun main(args: Array<String>) {
        val env = StreamExecutionEnvironment.getExecutionEnvironment().apply {
            parallelism = 1
        }

        val kafka = "localhost:9092"

        val logSource = KafkaSource.builder<String>()
            .setBootstrapServers(kafka)
            .setTopics("logs")
            .setGroupId("flink-correlator")
            .setStartingOffsets(OffsetsInitializer.latest())
            .setValueOnlyDeserializer(SimpleStringSchema())
            .build()

        val alertSource = KafkaSource.builder<String>()
            .setBootstrapServers(kafka)
            .setTopics("alerts")
            .setGroupId("flink-correlator")
            .setStartingOffsets(OffsetsInitializer.latest())
            .setValueOnlyDeserializer(SimpleStringSchema())
            .build()

        val incidentSink = KafkaSink.builder<String>()
            .setBootstrapServers(kafka)
            .setRecordSerializer(
                KafkaRecordSerializationSchema.builder<String>()
                    .setTopic("incidents")
                    .setValueSerializationSchema(SimpleStringSchema())
                    .build()
            )
            .build()

        val logs   = env.fromSource(logSource,   WatermarkStrategy.noWatermarks(), "logs-source")
        val alerts = env.fromSource(alertSource, WatermarkStrategy.noWatermarks(), "alerts-source")

        // group errors by service in 30s windows, emit an incident if 3+ errors
        logs
            .flatMap(LogParser())
            .filter { it.level == "ERROR" }
            .keyBy { it.service }
            .window(TumblingProcessingTimeWindows.of(Time.seconds(30)))
            .apply(LogCorrelator(), Types.STRING)
            .sinkTo(incidentSink)

        // every alert becomes an incident immediately
        alerts
            .flatMap(AlertParser())
            .flatMap(AlertToIncident())
            .map { jacksonObjectMapper().writeValueAsString(it) }
            .returns(Types.STRING)
            .sinkTo(incidentSink)

        log.info("Incident correlator started")
        env.execute("IncidentCorrelatorJob")
    }
}


// ── Data classes ──────────────────────────────────────────────────────────

data class ParsedLog(
    val id:        String,
    val service:   String,
    val level:     String,
    val message:   String,
    val host:      String,
    val timestamp: Long,
)

data class ParsedAlert(
    val id:       String,
    val name:     String,
    val source:   String,
    val severity: String,
    val service:  String,
    val message:  String,
    val firedAt:  Long,
)

data class CorrelatedIncident(
    val id:              String,
    val title:           String,
    val description:     String,
    val severity:        String,
    val status:          String = "OPEN",
    val service:         String,
    val startedAt:       Long,
    val relatedEventIds: List<String>,
    val source:          String,
)


// ── Parsers ───────────────────────────────────────────────────────────────

private val mapper = jacksonObjectMapper()

class LogParser : RichFlatMapFunction<String, ParsedLog>() {
    override fun flatMap(json: String, out: Collector<ParsedLog>) {
        runCatching {
            val node = mapper.readTree(json)
            out.collect(ParsedLog(
                id        = node["id"].asText(),
                service   = node["service"].asText("unknown"),
                level     = node["level"].asText("ERROR"),
                message   = node["message"].asText(""),
                host      = node["host"].asText("unknown"),
                timestamp = node["timestamp"].asLong(System.currentTimeMillis()),
            ))
        }.onFailure { LoggerFactory.getLogger(LogParser::class.java).warn("Failed to parse log: $json", it) }
    }
}

class AlertParser : RichFlatMapFunction<String, ParsedAlert>() {
    override fun flatMap(json: String, out: Collector<ParsedAlert>) {
        runCatching {
            val node = mapper.readTree(json)
            out.collect(ParsedAlert(
                id       = node["id"].asText(),
                name     = node["name"].asText("Unknown alert"),
                source   = node["source"].asText("custom"),
                severity = node["severity"].asText("HIGH"),
                service  = node["service"].asText("unknown"),
                message  = node["message"].asText(""),
                firedAt  = node["fired_at"].asLong(System.currentTimeMillis()),
            ))
        }.onFailure { LoggerFactory.getLogger(AlertParser::class.java).warn("Failed to parse alert: $json", it) }
    }
}

class AlertToIncident : RichFlatMapFunction<ParsedAlert, CorrelatedIncident>() {
    override fun flatMap(alert: ParsedAlert, out: Collector<CorrelatedIncident>) {
        out.collect(CorrelatedIncident(
            id              = "inc-${alert.id}",
            title           = alert.name,
            description     = "${alert.source} alert: ${alert.message}",
            severity        = alert.severity,
            service         = alert.service,
            startedAt       = alert.firedAt,
            relatedEventIds = listOf(alert.id),
            source          = "alert",
        ))
    }
}

class LogCorrelator : WindowFunction<ParsedLog, String, String, TimeWindow> {
    private val log    = LoggerFactory.getLogger(LogCorrelator::class.java)
    private val mapper = jacksonObjectMapper()

    override fun apply(
        service: String,
        window:  TimeWindow,
        input:   Iterable<ParsedLog>,
        out:     Collector<String>
    ) {
        val events = input.toList()
        if (events.size < 3) return

        val severity = when {
            events.size >= 10 -> "CRITICAL"
            events.size >= 6  -> "HIGH"
            else              -> "MEDIUM"
        }

        val incident = CorrelatedIncident(
            id              = "inc-${java.util.UUID.randomUUID()}",
            title           = "$service — ${events.size} errors in 30s",
            description     = events.take(3).joinToString("; ") { it.message.take(80) },
            severity        = severity,
            service         = service,
            startedAt       = events.minOf { it.timestamp },
            relatedEventIds = events.map { it.id },
            source          = "log-correlation",
        )

        log.info("Incident created for service={} errors={}", service, events.size)
        out.collect(mapper.writeValueAsString(incident))
    }
}