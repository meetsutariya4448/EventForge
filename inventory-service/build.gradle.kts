plugins {
    id("eventforge.spring-service-conventions")
}

dependencies {
    implementation(project(":common-events"))
    implementation("org.springframework.boot:spring-boot-starter-data-jpa")
    testImplementation(project(":common-testing"))
}
