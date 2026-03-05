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
            .waitingFor("query-processor",
                Wait.forHttp("/health")
                    .forStatusCode(200)
                    .withStartupTimeout(Duration.ofSeconds(120)));

    public static synchronized void start() {
        if (started) return;
        System.out.println("[KoopCluster] Starting...");
        ENVIRONMENT.start();
        started = true;
        System.out.println("[KoopCluster] UP");
    }

    public static String baseUrl()  { return "http://localhost:9001"; }
    public static String baseUrl2() { return "http://localhost:9002"; }
    public static String baseUrl3() { return "http://localhost:9003"; }
}