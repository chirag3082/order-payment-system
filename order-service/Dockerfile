# Multi-stage build: compile with Maven, run on a slim JRE.
FROM maven:3.9-eclipse-temurin-21 AS build
WORKDIR /workspace
COPY pom.xml .
COPY common/pom.xml common/pom.xml
COPY order-service/pom.xml order-service/pom.xml
COPY payment-service/pom.xml payment-service/pom.xml
# Warm the dependency cache (best-effort; ignore failure on first run).
RUN mvn -q -pl common,order-service -am dependency:go-offline || true
COPY common common
COPY order-service order-service
RUN mvn -q -pl order-service -am package -DskipTests

FROM eclipse-temurin:21-jre
WORKDIR /app
COPY --from=build /workspace/order-service/target/order-service.jar app.jar
EXPOSE 8081
ENTRYPOINT ["java", "-jar", "app.jar"]
