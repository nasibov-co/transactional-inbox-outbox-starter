import org.gradle.testing.jacoco.plugins.JacocoCoverageReport
import org.gradle.testing.jacoco.plugins.JacocoTaskExtension
import org.gradle.testing.jacoco.tasks.JacocoReport

plugins {
    kotlin("jvm") version "2.2.21" apply false
    kotlin("plugin.spring") version "2.2.21" apply false
    kotlin("kapt") version "2.2.21" apply false
    id("org.springframework.boot") version "4.0.6" apply false
    id("com.vanniktech.maven.publish") version "0.36.0" apply false
    id("jacoco-report-aggregation")
}

group = "com.fnasibov"
version = "5.1.3"

allprojects {
    group = rootProject.group
    version = rootProject.version

    repositories {
        mavenCentral()
    }
}

// Root-level aggregate report across all modules. Produces `testCodeCoverageReport`
// (HTML + XML) under build/reports/jacoco/. Modules without test sources contribute an
// empty dataset instead of failing the aggregation.
dependencies {
    jacocoAggregation(project(":transactional-inbox-outbox-core"))
    jacocoAggregation(project(":transactional-inbox-outbox-jdbc"))
    jacocoAggregation(project(":transactional-inbox-outbox-r2dbc"))
    jacocoAggregation(project(":transactional-inbox-outbox-autoconfigure"))
    jacocoAggregation(project(":transactional-inbox-outbox-demo"))
    jacocoAggregation(project(":transactional-inbox-outbox-demo-jdbc"))
    jacocoAggregation(project(":transactional-inbox-outbox-starter-jdbc"))
    jacocoAggregation(project(":transactional-inbox-outbox-starter-r2dbc"))
}

reporting {
    reports {
        val testCodeCoverageReport by creating(JacocoCoverageReport::class) {
            testSuiteName = "test"
        }
    }
}

// Test coverage for every Java/Kotlin module. Modules without test sources still get the
// jacocoTestReport task, which simply produces an empty report instead of failing.
subprojects {
    plugins.withId("java") {
        apply(plugin = "jacoco")

        tasks.withType<Test>().configureEach {
            extensions.configure<JacocoTaskExtension> {
                isEnabled = true
            }
        }

        tasks.withType<JacocoReport>().configureEach {
            dependsOn(tasks.named("test"))
            reports {
                html.required.set(true)
                xml.required.set(true)
                html.outputLocation.set(layout.buildDirectory.dir("reports/jacoco/test/html"))
                xml.outputLocation.set(layout.buildDirectory.file("reports/jacoco/test/jacocoTestReport.xml"))
            }
        }
    }
}
