val ktorVersion = "2.3.4"
val exposedVersion = "0.43.0"
val h2Version = "2.2.222"
val hikariCpVersion = "5.0.1"
val flywayVersion = "9.22.0"
val logbackVersion = "1.4.11"
val assertjVersion = "3.24.2"
val restAssuredVersion = "5.3.1"
val junitVersion = "5.9.1"

plugins {
    kotlin("jvm") version "1.9.0"
    kotlin("plugin.serialization") version "1.9.0"
    id("org.jetbrains.kotlinx.kover") version "0.7.3"
    application
}

repositories {
    mavenCentral()
}

dependencies {
    implementation("io.ktor:ktor-server-core:$ktorVersion")
    implementation("io.ktor:ktor-serialization:$ktorVersion")
    implementation("io.ktor:ktor-server-netty:$ktorVersion")
    implementation("io.ktor:ktor-server-call-logging:$ktorVersion")
    implementation("io.ktor:ktor-server-default-headers:$ktorVersion")
    implementation("io.ktor:ktor-server-websockets:$ktorVersion")
    implementation("io.ktor:ktor-server-content-negotiation:$ktorVersion")
    implementation("io.ktor:ktor-serialization-kotlinx-json:$ktorVersion")

    implementation("com.h2database:h2:$h2Version")
    implementation("org.jetbrains.exposed:exposed-core:$exposedVersion")
    implementation("org.jetbrains.exposed:exposed-jdbc:$exposedVersion")
    implementation("com.zaxxer:HikariCP:$hikariCpVersion")
    implementation("org.flywaydb:flyway-core:$flywayVersion")
    implementation("ch.qos.logback:logback-classic:$logbackVersion")

    testImplementation("org.assertj:assertj-core:$assertjVersion")
    testImplementation("io.rest-assured:rest-assured:$restAssuredVersion")
    testImplementation("org.junit.jupiter:junit-jupiter-api:$junitVersion")
    testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine:$junitVersion")
    testImplementation("io.ktor:ktor-client-cio:$ktorVersion")
}

application {
    mainClass.set("MainKt")
}

tasks.withType<Test> {
    useJUnitPlatform()
}
