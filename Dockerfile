# syntax=docker/dockerfile:1
# check=error=true

FROM hsldevcom/infodevops-docker-base-images:1.0.2-25-java-jdk AS build
WORKDIR /usr/app
ARG GITHUB_ACTOR=github-actions
COPY gradlew ./
COPY gradle gradle
COPY build.gradle settings.gradle ./
COPY src src
RUN --mount=type=secret,id=github_token \
    export GITHUB_TOKEN="$(cat /run/secrets/github_token)" && \
    export GITHUB_ACTOR="$GITHUB_ACTOR" && \
    ./gradlew --no-daemon shadowJar

FROM hsldevcom/infodevops-docker-base-images:1.0.2-25-java-jre
COPY --from=build /usr/app/build/libs/transitdata-eke-sink.jar /usr/app/transitdata-eke-sink.jar
ENTRYPOINT ["java", "-XX:InitialRAMPercentage=25.0", "-XX:MaxRAMPercentage=75.0", "-jar", "/usr/app/transitdata-eke-sink.jar"]
