/*
 * This file was generated by the Gradle 'init' task.
 *
 * This generated file contains a sample Kotlin application project to get you started.
 * For more details take a look at the 'Building Java & JVM projects' chapter in the Gradle
 * User Manual available at https://docs.gradle.org/7.2/userguide/building_java_projects.html
 */

plugins {
    // Apply the org.jetbrains.kotlin.jvm Plugin to add support for Kotlin.
    id("org.jetbrains.kotlin.jvm") version "1.5.0"

    // Gradle Test Logger Plugin
    id("com.adarshr.test-logger") version "3.0.0"

    // Use protobuf plugin
    id("com.google.protobuf") version "0.8.17"

    // Use the integration test plugin
    id("com.coditory.integration-test") version "1.3.0"

    // Apply the application plugin to add support for building a CLI application in Java.
    application
}

repositories {
    maven { url = uri("https://packages.confluent.io/maven/") }
    // Use Maven Central for resolving dependencies.
    mavenCentral()
}

dependencies {
   // Align versions of all Kotlin components
   implementation(platform("org.jetbrains.kotlin:kotlin-bom"))
   implementation("org.jetbrains.kotlin:kotlin-reflect:1.5.30")
   implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.5.2")
   
   // Use the Kotlin JDK 8 standard library.
   implementation("org.jetbrains.kotlin:kotlin-stdlib-jdk8")

   // This dependency is used by the application.
   implementation("com.google.guava:guava:30.1.1-jre")

   implementation("org.slf4j:slf4j-api:1.7.30")
   implementation("org.slf4j:slf4j-jdk14:1.7.30")
   implementation("org.apache.beam:beam-sdks-java-core:2.32.0")
   implementation("org.apache.beam:beam-runners-direct-java:2.32.0")
   implementation("org.apache.beam:beam-runners-google-cloud-dataflow-java:2.32.0")
   implementation("org.apache.beam:beam-sdks-java-io-google-cloud-platform:2.32.0")
   // protobuf
   implementation("com.google.protobuf:protobuf-java:3.0.0")

   // Use junit for testing
   testImplementation(kotlin("test-junit5"))
   testImplementation("org.junit.jupiter:junit-jupiter-params:5.0.0")
}

application {
    // Define the main class for the application.
    mainClass.set("dataflow.e2e.demo.AppKt")
}

tasks.test { 
    useJUnitPlatform() 
    outputs.upToDateWhen { false }
}

tasks.integrationTest {
    useJUnitPlatform()
    onlyIf { project.hasProperty("e2e") }
    outputs.upToDateWhen { false }
}

testlogger {
    showStandardStreams = project.hasProperty("showOutput")
}
