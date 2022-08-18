FROM eclipse-temurin:11-alpine
#Install curl for health check
RUN apk add --no-cache curl

ADD build/libs/transitdata-eke-sink.jar /usr/app/transitdata-eke-sink.jar
ENTRYPOINT ["java", "-XX:InitialRAMPercentage=25.0", "-XX:MaxRAMPercentage=90.0", "-jar", "/usr/app/transitdata-eke-sink.jar"]
