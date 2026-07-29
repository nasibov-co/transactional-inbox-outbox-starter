plugins {
    kotlin("jvm") version "2.2.21" apply false
    kotlin("plugin.spring") version "2.2.21" apply false
    kotlin("kapt") version "2.2.21" apply false
    id("org.springframework.boot") version "4.0.6" apply false
    id("com.vanniktech.maven.publish") version "0.36.0" apply false
}

group = "com.fnasibov"
version = "3.0.0"

allprojects {
    group = rootProject.group
    version = rootProject.version

    repositories {
        mavenCentral()
    }
}
