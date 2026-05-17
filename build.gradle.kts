plugins {
	kotlin("jvm") version "2.2.21"
	kotlin("plugin.spring") version "2.2.21"
	`java-library`
	`maven-publish`
}

group = "com.fnasibov"
version = "0.0.1"

java {
	withSourcesJar()
	withJavadocJar()
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


publishing {
	publications {
		create<MavenPublication>("mavenJava") {
			from(components["java"])
			groupId = project.group.toString()
			artifactId = "transactional-inbox-outbox-starter-r2dbc"
			version = project.version.toString()
		}
	}
	repositories {
		maven {
			name = "OSSRH"
			url = uri("https://oss.sonatype.org/service/local/staging/deploy/maven2/")

			credentials {
				username = System.getenv("MAVEN_USERNAME")
				password = System.getenv("MAVEN_PASSWORD")
			}
		}
	}
}