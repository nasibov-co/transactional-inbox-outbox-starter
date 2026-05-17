plugins {
	kotlin("jvm") version "2.2.21"
	kotlin("plugin.spring") version "2.2.21"
	`java-library`
	id("com.vanniktech.maven.publish") version "0.36.0"
}

group = "com.fnasibov"
version = "0.0.1"

java {
	toolchain {
		languageVersion = JavaLanguageVersion.of(21)
	}
}

repositories {
	mavenCentral()
}

dependencies {
	implementation(platform("org.springframework.boot:spring-boot-dependencies:4.0.6"))
	api("org.springframework.boot:spring-boot-autoconfigure")
	api("org.springframework.boot:spring-boot-starter-data-r2dbc")
	implementation("io.projectreactor.kotlin:reactor-kotlin-extensions")
	implementation("io.github.oshai:kotlin-logging-jvm:8.0.02")
	implementation("org.jetbrains.kotlin:kotlin-reflect")
	implementation("org.jetbrains.kotlinx:kotlinx-coroutines-reactor")
	annotationProcessor("org.springframework.boot:spring-boot-configuration-processor")
	testImplementation("org.springframework.boot:spring-boot-starter-r2dbc-test")
	testImplementation("org.jetbrains.kotlin:kotlin-test-junit5")
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

	coordinates("io.github.fnasibov", "transactional-inbox-outbox-starter-r2dbc", project.version.toString())

	pom {
		name.set("Transactional Inbox Outbox Starter")
		description.set("A lightweight Spring Boot starter for implementing the Transactional Outbox / Inbox pattern using R2DBC + Coroutines.")
		inceptionYear.set("2026")
		url.set("https://github.com/fnasibov/transactional-inbox-outbox-starter-r2dbc")
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
			url.set("https://github.com/fnasibov/transactional-inbox-outbox-starter-r2dbc")
		}
	}
}