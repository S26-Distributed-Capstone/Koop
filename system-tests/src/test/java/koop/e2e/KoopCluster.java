package koop.e2e;

import java.io.File;
import java.net.Socket;

/**
 * Manages the full KoopDB cluster lifecycle for E2E tests.
 * Starts docker-compose once for the entire test run and tears it down on JVM exit.
 *
 * Uses the existing docker-compose.yml at the repo root — no duplication.
 */
public class KoopCluster {

    // The query processor is exposed on these ports by docker-compose.yml
    public static final String QP_BASE_URL = "http://localhost:9001";
    public static final String QP_BASE_URL_2 = "http://localhost:9002";
    public static final String QP_BASE_URL_3 = "http://localhost:9003";

    private static final File COMPOSE_FILE =
        new File(System.getProperty("compose.file", "../docker-compose.yml"));

    private static volatile boolean started = false;

    static {
        start();
        Runtime.getRuntime().addShutdownHook(new Thread(KoopCluster::stop));
    }

    private static void start() {
        if (started) return;
        try {
            log("Starting KoopDB cluster via docker-compose...");
            exec("docker", "compose", "-f", COMPOSE_FILE.getPath(), "up", "--build", "-d");
            waitForHealth("localhost", 9001, 120_000);
            waitForHealth("localhost", 9002, 30_000);
            waitForHealth("localhost", 9003, 30_000);
            started = true;
            log("Cluster is UP");
        } catch (Exception e) {
            throw new RuntimeException("Failed to start KoopDB cluster", e);
        }
    }

    private static void stop() {
        try {
            log("Stopping KoopDB cluster...");
            exec("docker", "compose", "-f", COMPOSE_FILE.getPath(), "down", "-v");
            log("Cluster is DOWN");
        } catch (Exception e) {
            System.err.println("Warning: failed to stop cluster: " + e.getMessage());
        }
    }

    /**
     * Waits until a TCP port is accepting connections.
     * This is more reliable than HTTP health checks during early startup.
     */
    private static void waitForHealth(String host, int port, long timeoutMs)
            throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            try (Socket s = new Socket(host, port)) {
                log("Port " + port + " is open");
                return;
            } catch (Exception ignored) {
                Thread.sleep(500);
            }
        }
        throw new RuntimeException("Timed out waiting for " + host + ":" + port);
    }

    private static void exec(String... cmd) throws Exception {
        Process p = new ProcessBuilder(cmd)
            .inheritIO() // print docker-compose output to your console
            .start();
        int exit = p.waitFor();
        if (exit != 0) throw new RuntimeException("Command failed with exit " + exit);
    }

    private static void log(String msg) {
        System.out.println("[KoopCluster] " + msg);
    }
}