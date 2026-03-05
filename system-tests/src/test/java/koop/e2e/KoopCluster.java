package koop.e2e;

import org.testcontainers.containers.ComposeContainer;
import org.testcontainers.containers.wait.strategy.Wait;

import java.io.File;
import java.time.Duration;

public class KoopCluster {

    private static final File COMPOSE_FILE = new File("../docker-compose.yml");
    private static volatile boolean started = false;

    private static final ComposeContainer ENVIRONMENT =
        new ComposeContainer(COMPOSE_FILE)
            .withBuild(true)
            // withExposedService registers the container port with Testcontainers
            // so it can resolve the mapped host port. Without this, getMappedPort()
            // returns null and waitingFor throws a NullPointerException.
            // The port here is the container-internal port (8080), not the host port.
            .withExposedService("query_processor_1", 8080)
            .withExposedService("query_processor_2", 8080)
            .withExposedService("query_processor_3", 8080)
            // waitingFor must come AFTER withExposedService for the same service
            .waitingFor("query_processor_1",
                Wait.forHttp("/health")
                    .forStatusCode(200)
                    .withStartupTimeout(Duration.ofSeconds(180)))
            .waitingFor("query_processor_2",
                Wait.forHttp("/health")
                    .forStatusCode(200)
                    .withStartupTimeout(Duration.ofSeconds(180)))
            .waitingFor("query_processor_3",
                Wait.forHttp("/health")
                    .forStatusCode(200)
                    .withStartupTimeout(Duration.ofSeconds(180)));

    public static synchronized void start() {
        if (started) return;
        System.out.println("[KoopCluster] Building images and starting cluster (this takes 3-5 min on first run)...");
        System.out.println("[KoopCluster] Waiting for: etcd x3, storage nodes x9, query processors x3, redis");

        long startMs = System.currentTimeMillis();
        ENVIRONMENT.start();
        long elapsedSec = (System.currentTimeMillis() - startMs) / 1000;

        started = true;
        System.out.println("[KoopCluster] UP — cluster ready in " + elapsedSec + "s");
    }

    // Ports 9001-9003 are statically mapped in docker-compose.yml.
    public static String baseUrl()  { return "http://localhost:9001"; }
    public static String baseUrl2() { return "http://localhost:9002"; }
    public static String baseUrl3() { return "http://localhost:9003"; }
}