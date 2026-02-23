FROM maven:3.9.6-eclipse-temurin-21 AS builder

WORKDIR /build

# copy whole project (needed for multi-module build)
COPY . .

# build only service-1 + its dependencies
RUN mvn -pl storage-node -am clean package -DskipTests

# ---- Runtime stage ----
FROM eclipse-temurin:21-jdk
WORKDIR /app

COPY --from=builder /build/storage-node/target/storage-node-*-jar-with-dependencies.jar app.jar

ENTRYPOINT ["java","-jar","/app/app.jar"]