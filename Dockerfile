FROM openjdk:8-jre-slim
#Install curl for health check
RUN apt-get update && apt-get install -y --no-install-recommends curl
ADD build/libs/transitdata-eke-sink.jar /usr/app/transitdata-eke-sink.jar
ENTRYPOINT ["java", "-Xms256m", "-Xmx4096m", "-jar", "/usr/app/transitdata-eke-sink.jar"]
