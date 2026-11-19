FROM hsldevcom/infodevops-docker-base-images:1.0.2-25-java-jre

ADD build/libs/transitdata-eke-sink.jar /usr/app/transitdata-eke-sink.jar
ENTRYPOINT ["java", "-XX:InitialRAMPercentage=25.0", "-XX:MaxRAMPercentage=75.0", "-jar", "/usr/app/transitdata-eke-sink.jar"]
