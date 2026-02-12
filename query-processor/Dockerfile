# --- Stage 1: Build ---
FROM maven:3.9.6-eclipse-temurin-21 AS build
WORKDIR /app
COPY pom.xml .
COPY src ./src
RUN mvn clean package -DskipTests && \
    mv target/*-jar-with-dependencies.jar target/app.jar

# --- Stage 2: Runtime ---
FROM eclipse-temurin:21-jre
WORKDIR /app
COPY --from=build /app/target/app.jar app.jar

ENTRYPOINT ["java", "-jar", "/app/app.jar"]