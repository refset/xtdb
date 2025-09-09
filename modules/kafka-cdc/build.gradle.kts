import org.jetbrains.kotlin.gradle.dsl.JvmTarget

plugins {
    `java-library`
    id("dev.clojurephant.clojure")
    `maven-publish`
    signing
    kotlin("jvm")
    kotlin("plugin.serialization")
    id("org.jetbrains.dokka")
}

publishing {
    publications.create("maven", MavenPublication::class) {
        pom {
            name.set("XTDB Kafka CDC")
            description.set("XTDB Kafka CDC with Debezium-compatible Avro output")
        }
    }
}

java.toolchain.languageVersion.set(JavaLanguageVersion.of(21))

dependencies {
    api(project(":xtdb-api"))
    api(project(":xtdb-core"))
    api(project(":modules:xtdb-kafka"))

    // Kafka dependencies
    api("org.apache.kafka", "kafka-clients", "4.0.0")
    
    // Avro dependencies for serialization
    api("org.apache.avro", "avro", "1.11.3")
    api("io.confluent", "kafka-avro-serializer", "7.5.0")
    api("io.confluent", "kafka-schema-registry-client", "7.5.0")
    
    // JSON processing
    api("com.fasterxml.jackson.core", "jackson-databind", "2.15.2")
    api("com.fasterxml.jackson.module", "jackson-module-kotlin", "2.15.2")
    
    // Metrics
    api("io.micrometer", "micrometer-core", "1.11.4")

    api(kotlin("stdlib-jdk8"))
    implementation(libs.kotlinx.coroutines)
    
    testImplementation(libs.mockk)
    testImplementation(libs.kotlinx.coroutines.test)
    testImplementation("org.testcontainers", "kafka", "1.19.0")
    testImplementation("org.junit.jupiter", "junit-jupiter", "5.10.0")
}

repositories {
    maven {
        url = uri("https://packages.confluent.io/maven/")
    }
}