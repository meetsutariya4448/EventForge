plugins {
    id("eventforge.spring-service-conventions")
}

dependencies {
    implementation(project(":common-events"))
    testImplementation(project(":common-testing"))
}
