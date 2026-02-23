FROM maven:3.9.6-eclipse-temurin-21 AS builder

WORKDIR /build

# copy whole project (needed for multi-module build)
COPY . .

# build only service-1 + its dependencies
RUN mvn -pl query-processor -am clean package -DskipTests

# ---- Runtime stage ----
FROM eclipse-temurin:21-jdk
WORKDIR /app

COPY --from=builder /build/query-processor/target/query-processor-*-jar-with-dependencies.jar app.jar

ENTRYPOINT ["java","-jar","/app/app.jar"]