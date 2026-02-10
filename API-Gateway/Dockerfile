# Build Stage
FROM maven:3.9.12-eclipse-temurin-21 AS build
WORKDIR /app
COPY pom.xml .
COPY src ./src
RUN mvn clean package -DskipTests

# Run Stage
FROM eclipse-temurin:21-jre-alpine
WORKDIR /app

# [New] Create a non-root user for security
RUN addgroup -S koop && adduser -S koop -G koop
USER koop

# Copy the JAR
COPY --from=build /app/target/api-gateway-1.0.0.jar app.jar

# [New] Set defaults for your new configuration
ENV STORAGE_HOST=localhost
ENV STORAGE_PORT=8080
ENV PORT=8080

EXPOSE 8080
CMD ["java", "-jar", "app.jar"]