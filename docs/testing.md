# Testing Conventions

This page is the contract for how tests are written, named, and run in this
repo. Keep it short. If you want to deviate, update this doc first.

## Test tiers

We have two tiers, distinguished by file suffix:

| Suffix      | Plugin    | Allowed dependencies                                          | Speed budget    |
| ----------- | --------- | ------------------------------------------------------------- | --------------- |
| `*Test`     | Surefire  | JUnit 5, Mockito, in-memory fakes, `@TempDir`, embedded HTTP  | < 2 s / test    |
| `*IT`       | Failsafe  | Above + Testcontainers, multi-process / multi-node setups     | < 60 s / test   |

End-to-end tests in the `system-tests` module use the suffix `*E2EIT` so they
are easy to identify in reports. They are still picked up by Failsafe.

If a test outgrows its tier (e.g. a `*Test` adds a Testcontainer), rename it.

## Naming

**Class name:** `<ClassUnderTest>Test`, `<Subsystem>IT`, or `<Subsystem>E2EIT`.

**Method name:** `methodOrScenario_condition_expectedResult`.

Examples:

```
store_thenRetrieve_returnsSamePayload
putObject_whenQuorumLost_returnsServiceUnavailable
initialFetch_returnsExistingData
listObjects_withPrefix_filtersTombstones
```

Avoid `testXxx` and SDK-style `sdkXxx_yyy`. Existing tests get renamed
opportunistically when touched, not in a sweep.

## Display names

Add this to every test class so reports read as English:

```java
@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
class StorageNodeV2Test { ... }
```

`store_thenRetrieve_returnsSamePayload` → `store then retrieve returns same payload`.

## Logging

Each module has a single `src/test/resources/log4j2-test.xml` with the
**same** pattern and root level. The pattern is:

```
%d{HH:mm:ss.SSS} %-5level [%t] %logger{0} - %msg%n
```

Root level is `INFO`. The shared config silences known-noisy categories
(`org.apache.kafka`, `io.netty`, `org.testcontainers`, `software.amazon.awssdk`)
to `WARN`.

To debug locally without editing the file, pass `-Dlog4j2.level=DEBUG` to Maven.

Do not `System.out.println` from tests. Use a `Logger`.

## Synchronization

`Thread.sleep` is forbidden as a way to wait for an event. Use
[Awaitility](https://github.com/awaitility/awaitility):

```java
import static org.awaitility.Awaitility.await;
import static java.time.Duration.ofSeconds;

await().atMost(ofSeconds(10)).until(() -> server.repairPendingCount() == 0);
```

`Thread.sleep` is acceptable only when asserting the **absence** of an event
within a fixed window (e.g. "no double-execution after 150 ms").

## Test doubles

- **Mockito** for collaborators with a stable interface (services, workers).
- **In-memory fakes** (`MemoryFetcher`, `MemoryPubSub`, `MemoryCacheClient`)
  for infrastructure with semantic state. They live next to the tests that
  use them today; we'll consolidate later.
- **Real components** for boundaries: `@TempDir` + RocksDB, embedded Javalin
  on a random port.
- **No reflection** on production classes. If a private method needs a test,
  make it package-private and put the test in the same package.

## Lifecycle

- Default to `@TestInstance(PER_METHOD)` (the JUnit default).
- `PER_CLASS` is allowed only when the class owns an expensive resource
  (a Testcontainer, a multi-node cluster). Pair it with explicit `@BeforeEach`
  cleanup so tests stay independent.
- `@TestMethodOrder` + `@Order` is forbidden. Tests must not depend on
  execution order.

## Running tests

- Unit tests only:        `mvn test`
- Unit + integration:     `mvn verify`
- Single module:          `mvn -pl query-processor verify`
- Single class:           `mvn -pl query-processor test -Dtest=S3Test`
- Single method:          `mvn -pl query-processor test -Dtest=S3Test#sdkPutObject_returns200_noException`

Note: full `mvn verify` is slow because of integration tests. Prefer module-
or class-scoped runs during development.
