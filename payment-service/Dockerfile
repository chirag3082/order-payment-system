# Multi-stage build: compile with Maven, run on a slim JRE.
FROM maven:3.9-eclipse-temurin-21 AS build
WORKDIR /workspace
COPY pom.xml .
COPY common/pom.xml common/pom.xml
COPY order-service/pom.xml order-service/pom.xml
COPY payment-service/pom.xml payment-service/pom.xml
RUN mvn -q -pl common,payment-service -am dependency:go-offline || true
COPY common common
COPY payment-service payment-service
RUN mvn -q -pl payment-service -am package -DskipTests

FROM eclipse-temurin:21-jre
WORKDIR /app
COPY --from=build /workspace/payment-service/target/payment-service.jar app.jar
EXPOSE 8082
ENTRYPOINT ["java", "-jar", "app.jar"]
