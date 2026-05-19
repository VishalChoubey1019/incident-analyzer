plugins {
    kotlin("jvm") version "1.9.23"
    id("com.gradleup.shadow") version "9.0.0-beta4"
}

group   = "com.incidentanalyzer"
version = "0.1.0"

repositories {
    mavenCentral()
}

val flinkVersion = "1.18.1"

dependencies {
    // Flink core (provided by cluster at runtime, needed to compile)
    compileOnly("org.apache.flink:flink-streaming-java:$flinkVersion")
    compileOnly("org.apache.flink:flink-clients:$flinkVersion")

    // Kafka connector
    implementation("org.apache.flink:flink-connector-kafka:3.1.0-1.18")

    // JSON
    implementation("com.fasterxml.jackson.module:jackson-module-kotlin:2.17.0")
    implementation("com.fasterxml.jackson.core:jackson-databind:2.17.0")

    // Logging
    implementation("org.slf4j:slf4j-api:2.0.13")
    runtimeOnly("org.apache.logging.log4j:log4j-slf4j2-impl:2.23.1")

    // Kotlin
    implementation(kotlin("stdlib"))
}

tasks.shadowJar {
    archiveBaseName.set("flink-processor")
    archiveClassifier.set("")
    archiveVersion.set(version.toString())
    manifest { attributes["Main-Class"] = "com.incidentanalyzer.IncidentCorrelatorJob" }
    mergeServiceFiles()
    exclude("META-INF/*.SF", "META-INF/*.DSA", "META-INF/*.RSA")
    duplicatesStrategy = DuplicatesStrategy.EXCLUDE
    dependencies {
        exclude(dependency("org.apache.flink:flink-streaming-java"))
        exclude(dependency("org.apache.flink:flink-clients"))
    }
}

kotlin {
    jvmToolchain(17)
}
