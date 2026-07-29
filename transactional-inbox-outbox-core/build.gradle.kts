plugins {
    kotlin("jvm")
    kotlin("plugin.spring")
    kotlin("kapt")
    `java-library`
    id("com.vanniktech.maven.publish")
}

java {
    toolchain {
        languageVersion = JavaLanguageVersion.of(21)
    }
}

dependencies {
    implementation(platform("org.springframework.boot:spring-boot-dependencies:4.0.6"))
    kapt(platform("org.springframework.boot:spring-boot-dependencies:4.0.6"))

    api("org.springframework.boot:spring-boot-autoconfigure")
    api("org.springframework.data:spring-data-commons")
    api("io.micrometer:micrometer-core")

    implementation("org.springframework.boot:spring-boot-starter-validation")
    implementation("io.github.oshai:kotlin-logging-jvm:8.0.02")
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-reactor")
    compileOnly("org.springframework.boot:spring-boot-health")

    kapt("org.springframework.boot:spring-boot-configuration-processor")

    testImplementation("org.springframework.boot:spring-boot-starter-test")
    testImplementation("org.jetbrains.kotlin:kotlin-test-junit5")
    testImplementation("io.mockk:mockk:1.14.9")
    testImplementation("org.jetbrains.kotlinx:kotlinx-coroutines-test")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

kotlin {
    compilerOptions {
        freeCompilerArgs.addAll("-Xjsr305=strict", "-Xannotation-default-target=param-property")
    }
}

tasks.withType<Test> {
    useJUnitPlatform()
}

mavenPublishing {
    publishToMavenCentral()
    signAllPublications()
    coordinates("io.github.fnasibov", "transactional-inbox-outbox-core", project.version.toString())
}
