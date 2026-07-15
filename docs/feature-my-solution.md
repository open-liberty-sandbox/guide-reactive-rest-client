# feature/my-solution — Change Log

This document summarises the five commits added on top of `prod` in the `feature/my-solution` branch.

---

## 1. Create the initial `InventoryClient` (`InventoryClient.java`)

**Commit:** `feat: create InventoryClient`

### What changed

`InventoryClient.java` was created as a CDI `@RequestScoped` bean that builds a JAX-RS client programmatically using `ClientBuilder`. It exposes two methods: `getSystems()` (synchronous) and `getSystem()` (reactive, using the default `CompletionStage` provider):

```java
public CompletionStage<Properties> getSystem(String hostname) {
    return ClientBuilder.newClient()
                        .target(baseUri)
                        .path("/inventory/systems")
                        .path(hostname)
                        .request()
                        .header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON)
                        .rx()
                        .get(Properties.class);
}
```

The base URI is injected via MicroProfile Config:

```java
@Inject
@ConfigProperty(name = "INVENTORY_BASE_URI", defaultValue = "http://localhost:9085")
private String baseUri;
```

### Why

`.rx()` with no arguments selects JAX-RS's built-in `CompletionStageRxInvoker`, which dispatches the HTTP `GET` on a managed thread and immediately returns a `CompletionStage<Properties>` handle. This is the default reactive provider shipped with JAX-RS — no extra dependencies are required. The caller gets a future-like object it can chain callbacks onto without blocking.

---

## 2. Create the initial `QueryResource` (`QueryResource.java`)

**Commit:** `feat: create QueryResource`

### What changed

`QueryResource.java` was created to expose a `GET /query/systemLoad` endpoint. It fans out per-host calls using the `CompletionStage` returned by `InventoryClient.getSystem()` and coordinates their completion with a `CountDownLatch`:

```java
public Map<String, Properties> systemLoad() {
    List<String> systems = inventoryClient.getSystems();
    CountDownLatch remainingSystems = new CountDownLatch(systems.size());
    final Holder systemLoads = new Holder();

    for (String system : systems) {
        inventoryClient.getSystem(system)
                       .thenAcceptAsync(p -> {
                           if (p != null) {
                               systemLoads.updateValues(p);
                           }
                           remainingSystems.countDown();
                       })
                       .exceptionally(ex -> {
                           remainingSystems.countDown();
                           ex.printStackTrace();
                           return null;
                       });
    }

    remainingSystems.await(30, TimeUnit.SECONDS);
    return systemLoads.getValues();
}
```

A private `Holder` inner class stores results safely across threads using `volatile` and `ConcurrentHashMap`.

### Why

`thenAcceptAsync` attaches a non-blocking callback that fires on a separate thread when the `CompletionStage` resolves, so the for-loop dispatches all requests concurrently rather than waiting on each one in turn. `exceptionally` mirrors a catch block — it ensures `countDown()` is always called even when a request fails, preventing the latch from stalling indefinitely.

---

## 3. Update the web client to use Jersey's RxJava provider (`InventoryClient.java`)

**Commit:** `feat: update the web client`

### What changed

`getSystem()` was updated from returning `CompletionStage<Properties>` to returning `rx.Observable<Properties>`. The Jersey `RxObservableInvokerProvider` is registered on the client and `RxObservableInvoker.class` is passed to `.rx()`:

```java
public Observable<Properties> getSystem(String hostname) {
    return ClientBuilder.newClient()
                        .target(baseUri)
                        .register(RxObservableInvokerProvider.class)
                        .path("/inventory/systems")
                        .path(hostname)
                        .request()
                        .header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON)
                        .rx(RxObservableInvoker.class)
                        .get(new GenericType<Properties>() { });
}
```

Two new Maven dependencies enable this:

```xml
<dependency>
    <groupId>org.glassfish.jersey.ext.rx</groupId>
    <artifactId>jersey-rx-client-rxjava</artifactId>
</dependency>
<dependency>
    <groupId>org.glassfish.jersey.ext.rx</groupId>
    <artifactId>jersey-rx-client-rxjava2</artifactId>
</dependency>
```

### Why

`Observable` is part of RxJava and supports richer composition operators (map, filter, merge, etc.) than the plain `CompletionStage` interface. It also supports backpressure through the sibling `Flowable` type, which is important when a producer can emit faster than consumers can process. Passing `RxObservableInvoker.class` to `.rx()` tells JAX-RS to delegate execution to the Jersey provider instead of the default invoker, so the return type resolves correctly.

---

## 4. Update the resource to handle `Observable` responses (`QueryResource.java`)

**Commit:** `feat: update QueryResource`

### What changed

The per-host callback was changed from `thenAcceptAsync()` (the `CompletionStage` API) to `subscribe()` (the RxJava `Observable` API):

```java
inventoryClient.getSystem(system)
               .subscribe(p -> {
                   if (p != null) {
                       systemLoads.updateValues(p);
                   }
                   remainingSystems.countDown();
               }, e -> {
                   remainingSystems.countDown();
                   e.printStackTrace();
               });
```

The `CountDownLatch` coordination and `Holder` result accumulator are unchanged.

### Why

`Observable.subscribe()` takes an onNext consumer and an onError consumer — a direct equivalent to `thenAcceptAsync` + `exceptionally` but expressed in RxJava idioms. Switching from `CompletionStage` callbacks to `subscribe()` is required because `Observable` does not implement `CompletionStage`; the two APIs are incompatible. The rest of the method (latch, await, Holder) remains the same because the coordination pattern is provider-agnostic.

---

## 5. Create the integration test (`QueryServiceIT.java`)

**Commit:** `feat: test the query microservice`

### What changed

`QueryServiceIT.java` was created to verify that the `query` microservice correctly identifies the host with the highest and lowest system load. It uses Testcontainers to spin up a MockServer container that stands in for the `inventory` service:

```java
@Test
public void testSystemLoad() {
    Map<String, Properties> response = client.systemLoad();
    assertEquals(
        "testHost2",
        response.get("highest").get("hostname"),
        "Returned highest system load incorrect"
    );
    assertEquals(
        "testHost1",
        response.get("lowest").get("hostname"),
        "Returned lowest system load incorrect"
    );
}
```

The `@BeforeEach` setup registers three mock hosts with distinct loads (`testHost1` → 1.23, `testHost2` → 3.21, `testHost3` → 2.13) so the test is deterministic regardless of any live `inventory` service. The `query` container is wired to call MockServer via the `INVENTORY_BASE_URI` environment variable:

```java
queryContainer.withEnv(
    "INVENTORY_BASE_URI",
    "http://mock-server:" + MockServerContainer.PORT);
```

### Why

Testing the `InventoryClient` interface in isolation would not exercise the RxJava `Observable` subscription, the `CountDownLatch` coordination, or the `Holder` aggregation together. By running the real `query` container against MockServer, the test exercises the full reactive call chain end-to-end. The three hosts with distinct loads give the assertion a clear expected winner (`testHost2` highest, `testHost1` lowest) that would fail if concurrent result collection produced a race condition or incorrect comparison.
