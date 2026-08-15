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

    api("org.springframework.boot:spring-boot")
    api("org.springframework.data:spring-data-commons")
    api("io.micrometer:micrometer-core")
    api("jakarta.validation:jakarta.validation-api")

    implementation("io.github.oshai:kotlin-logging-jvm:8.0.02")
    implementation(kotlin("reflect"))
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-reactor")
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

    pom {
        name.set("Transactional Inbox Outbox Core")
        description.set("Database-independent event processing, retry, and lifecycle infrastructure for the Transactional Outbox / Inbox pattern.")
        inceptionYear.set("2026")
        url.set("https://github.com/fnasibov/transactional-inbox-outbox-starter")
        licenses {
            license {
                name.set("The Apache License, Version 2.0")
                url.set("http://www.apache.org/licenses/LICENSE-2.0.txt")
            }
        }
        developers {
            developer {
                id.set("fnasibov")
                name.set("Fakhri Nasibov")
                email.set("fakhri.nasibov@gmail.com")
            }
        }
        scm {
            url.set("https://github.com/fnasibov/transactional-inbox-outbox-starter")
        }
    }
}
