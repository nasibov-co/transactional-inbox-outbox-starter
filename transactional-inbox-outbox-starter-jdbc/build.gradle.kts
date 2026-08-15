plugins {
    `java-library`
    id("com.vanniktech.maven.publish")
}

java {
    toolchain {
        languageVersion = JavaLanguageVersion.of(21)
    }
}

dependencies {
    api(platform("org.springframework.boot:spring-boot-dependencies:4.0.6"))
    api(project(":transactional-inbox-outbox-core"))
    api(project(":transactional-inbox-outbox-jdbc"))
    api(project(":transactional-inbox-outbox-autoconfigure"))
    api("org.springframework.boot:spring-boot-starter-data-jdbc")
    api("org.springframework.boot:spring-boot-starter-validation")
}

mavenPublishing {
    publishToMavenCentral()
    signAllPublications()
    coordinates("io.github.fnasibov", "transactional-inbox-outbox-starter-jdbc", project.version.toString())

    pom {
        name.set("Transactional Inbox Outbox JDBC Starter")
        description.set("Spring Boot starter for the Transactional Outbox / Inbox pattern using JDBC.")
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
