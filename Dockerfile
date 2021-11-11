FROM openjdk:8-jre-slim
#Install curl for health check
RUN apt-get update && apt-get install -y --no-install-recommends curl
ADD build/libs/transitdata-eke-sink.jar /usr/app/transitdata-eke-sink.jar
ENTRYPOINT ["java", "-XX:InitialRAMPercentage=25.0", "-XX:MaxRAMPercentage=90.0", "-jar", "/usr/app/transitdata-eke-sink.jar"]
