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

    api(project(":transactional-inbox-outbox-core"))
    api("org.springframework.boot:spring-boot-starter-data-r2dbc")

    implementation("io.projectreactor.kotlin:reactor-kotlin-extensions")
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-reactor")

    testImplementation("org.springframework.boot:spring-boot-starter-r2dbc-test")
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

    coordinates("io.github.fnasibov", "transactional-inbox-outbox-r2dbc", project.version.toString())

    pom {
        name.set("Transactional Inbox Outbox R2DBC")
        description.set("R2DBC persistence adapter for the Transactional Outbox / Inbox pattern.")
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
