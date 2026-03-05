package koop.e2e;

import io.restassured.RestAssured;
import org.junit.jupiter.api.BeforeAll;

/**
 * Base class for all E2E tests.
 * Guarantees the cluster is running before any test class executes.
 * Extend this instead of using @Testcontainers — the cluster boots once
 * for the whole suite, not once per class.
 */
public abstract class BaseE2EIT {

    @BeforeAll
    static void ensureClusterRunning() {
        KoopCluster.start(); // explicit call — no reliance on static initializers
        RestAssured.baseURI = KoopCluster.baseUrl(); // method call, not constant
    }
}