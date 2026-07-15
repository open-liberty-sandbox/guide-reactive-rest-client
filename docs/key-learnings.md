# Key Learnings — Quiz

Test your understanding of the Reactive JAX-RS Client guide.

---

**Q1. What are the three microservices in this guide and what does each one do?**

<details>
<summary>Answer</summary>

| Service                    | Port | Responsibility                                                                                                       |
| -------------------------- | ---- | -------------------------------------------------------------------------------------------------------------------- |
| **System Microservice**    | 9083 | Reads its own hostname and CPU load average every 15 seconds and publishes them to Kafka                             |
| **Inventory Microservice** | 9085 | Subscribes to Kafka for system load events and stores them; exposes REST endpoints to query the data                 |
| **Query Service**          | 9080 | Calls the Inventory Microservice via a reactive JAX-RS client to find the hosts with the highest and lowest CPU load |

</details>

---

**Q2. What are the two communication mechanisms used in this system, and which services use each?**

<details>
<summary>Answer</summary>

1. **Event-driven messaging (Apache Kafka)** — System Microservice → Inventory Microservice. Used for continuous, decoupled publishing of CPU metrics every 15 seconds.
2. **Reactive HTTP/REST (JAX-RS reactive client)** — Query Service → Inventory Microservice. Used on-demand when a client requests aggregated system load data.

The System Microservice declares its Kafka producer with `@Outgoing`, and the Inventory Microservice consumes it with `@Incoming`:

```java
// SystemService.java
@Outgoing("systemLoad")
public Publisher<SystemLoad> sendSystemLoad() {
    return Flowable.interval(15, TimeUnit.SECONDS)
                   .map(n -> new SystemLoad(getHostname(), OSMEAN.getSystemLoadAverage()));
}

// InventoryResource.java
@Incoming("systemLoad")
public void updateStatus(SystemLoad sl) {
    manager.addSystem(sl.hostname, sl.loadAverage);
}
```

The Query Service calls the Inventory Microservice via a programmatic JAX-RS client:

```java
// InventoryClient.java
public Observable<Properties> getSystem(String hostname) {
    return ClientBuilder.newClient()
                        .target(baseUri)
                        .register(RxObservableInvokerProvider.class)
                        ...
                        .rx(RxObservableInvoker.class)
                        .get(new GenericType<Properties>() { });
}
```

</details>

---

**Q3. What is the difference between the default JAX-RS reactive provider and the Jersey RxJava provider?**

<details>
<summary>Answer</summary>

The **default JAX-RS reactive provider** is built into the spec and requires no extra dependencies. It uses `CompletionStage` as the return type and the built-in `CompletionStageRxInvoker`. You select it by calling `.rx()` with no arguments:

```java
// Default provider — returns CompletionStage
public CompletionStage<Properties> getSystem(String hostname) {
    return ClientBuilder.newClient()
                        .target(baseUri)
                        ...
                        .rx()
                        .get(Properties.class);
}
```

The **Jersey RxJava provider** is a third-party extension. It returns `rx.Observable` (RxJava 1.x) and requires the `jersey-rx-client-rxjava` dependency. You select it by registering the provider class and passing the invoker class to `.rx()`:

```java
// Jersey RxJava provider — returns Observable
public Observable<Properties> getSystem(String hostname) {
    return ClientBuilder.newClient()
                        .target(baseUri)
                        .register(RxObservableInvokerProvider.class)
                        ...
                        .rx(RxObservableInvoker.class)
                        .get(new GenericType<Properties>() { });
}
```

`Observable` supports richer composition operators (map, filter, merge, zip) and the related `Flowable` type adds backpressure support — features not available on `CompletionStage`.

</details>

---

**Q4. What does `.rx()` do in a JAX-RS client invocation?**

<details>
<summary>Answer</summary>

`.rx()` switches the invocation from the synchronous `SyncInvoker` to a reactive invoker. When called with no arguments it returns the default `CompletionStageRxInvoker`; when called with a specific invoker class it returns that provider's invoker.

The reactive invoker dispatches the HTTP call on a managed thread pool thread and immediately returns a reactive type (either `CompletionStage` or the provider-specific type like `Observable`) so the calling thread is not blocked.

```java
// Without .rx() — blocking
Properties p = client.target(baseUri).path(...).request().get(Properties.class);

// With .rx() — non-blocking, returns CompletionStage
CompletionStage<Properties> cs = client.target(baseUri).path(...).request()
                                       .rx()
                                       .get(Properties.class);

// With .rx(RxObservableInvoker.class) — non-blocking, returns Observable
Observable<Properties> obs = client.target(baseUri).path(...).request()
                                   .register(RxObservableInvokerProvider.class)
                                   .rx(RxObservableInvoker.class)
                                   .get(new GenericType<Properties>() { });
```

</details>

---

**Q5. What is the difference between `subscribe()` (RxJava) and `thenAcceptAsync()` (`CompletionStage`)?**

<details>
<summary>Answer</summary>

Both attach a callback that fires asynchronously when the value becomes available. The key differences are API style and error handling:

| Aspect             | `CompletionStage.thenAcceptAsync()`      | `Observable.subscribe()`                                 |
| ------------------ | ---------------------------------------- | -------------------------------------------------------- |
| Success callback   | `.thenAcceptAsync(p -> { ... })`         | First lambda in `.subscribe(p -> { ... }, e -> { ... })` |
| Error callback     | Separate `.exceptionally(ex -> { ... })` | Second lambda in the same `.subscribe()` call            |
| Return type        | `CompletionStage<Void>`                  | `Subscription` (disposable)                              |
| Operator ecosystem | Limited (compose, thenApply, etc.)       | Full RxJava (map, filter, merge, zip, etc.)              |

```java
// CompletionStage style
inventoryClient.getSystem(system)
               .thenAcceptAsync(p -> { process(p); latch.countDown(); })
               .exceptionally(ex -> { latch.countDown(); return null; });

// Observable style
inventoryClient.getSystem(system)
               .subscribe(
                   p  -> { process(p); latch.countDown(); },
                   ex -> { latch.countDown(); ex.printStackTrace(); }
               );
```

</details>

---

**Q6. How does the Query Service know when all the async `getSystem()` calls have completed?**

<details>
<summary>Answer</summary>

It uses a `CountDownLatch` initialised to the number of hostnames. Each `subscribe()` callback — both the success and error paths — calls `countDown()` on the latch. The main thread calls `await(30, TimeUnit.SECONDS)`, blocking until the count reaches zero or the 30-second timeout expires.

```java
List<String> systems = inventoryClient.getSystems();
CountDownLatch remainingSystems = new CountDownLatch(systems.size());

for (String system : systems) {
    inventoryClient.getSystem(system)
                   .subscribe(
                       p  -> { systemLoads.updateValues(p); remainingSystems.countDown(); },
                       ex -> { ex.printStackTrace();        remainingSystems.countDown(); }
                   );
}

remainingSystems.await(30, TimeUnit.SECONDS); // block until all done (or timeout)
```

This is a fan-out / join pattern: dispatch all work in parallel, then join at the latch.

</details>

---

**Q7. Why does the Query Service use `ConcurrentHashMap` to collect results?**

<details>
<summary>Answer</summary>

Multiple `subscribe()` callbacks can complete and write to the map at the same time on different threads. A plain `HashMap` is not thread-safe and would produce unpredictable results under concurrent writes. `ConcurrentHashMap` provides thread-safe reads and writes without requiring explicit synchronisation.

```java
// Declared inside Holder — shared across all async callbacks
private volatile Map<String, Properties> values = new ConcurrentHashMap<>();

// Each callback runs on a different thread and writes concurrently
inventoryClient.getSystem(system)
               .subscribe(p -> {
                   systemLoads.updateValues(p);  // thread-safe write via ConcurrentHashMap
                   remainingSystems.countDown();
               }, ...);
```

The `volatile` keyword on the map reference ensures that the reference itself is visible across threads, while `ConcurrentHashMap` ensures individual operations on the map are atomic.

</details>

---

**Q8. The Query Service calls `getSystems()` synchronously but `getSystem()` reactively. Why the difference?**

<details>
<summary>Answer</summary>

`getSystems()` returns the list of hostnames that is needed _before_ any individual host queries can be dispatched — it cannot be parallelised. Once the list is in hand, all per-host `getSystem()` calls are independent of each other and can safely run concurrently, so the reactive (non-blocking) approach makes sense there.

```java
// Must finish first — synchronous is correct here
List<String> systems = inventoryClient.getSystems();

// Each of these is independent — all dispatched concurrently
for (String system : systems) {
    inventoryClient.getSystem(system)   // non-blocking Observable
                   .subscribe(...);
}
```

</details>

---

**Q9. What is RxJava backpressure, and how could it apply here?**

<details>
<summary>Answer</summary>

Backpressure is a mechanism that lets a consumer signal to a producer that it cannot keep up with the rate of emitted items, preventing unbounded buffering and out-of-memory errors.

In RxJava, `Observable` does **not** support backpressure — items are emitted as fast as the source produces them. The sibling type `Flowable` implements the Reactive Streams spec and supports backpressure strategies such as dropping items, buffering with a bounded queue, or signalling an error when the buffer is full.

In this guide, each `getSystem()` call emits a single item (one HTTP response), so backpressure is not a practical concern. However, if the pattern were extended to stream many items per host (e.g. a live metrics feed), switching from `Observable` to `Flowable` with a backpressure strategy would prevent the consumer from being overwhelmed. Jersey also provides `jersey-rx-client-rxjava2` for `Flowable`-based clients.

</details>

---

**Q10. How is the base URI for the Inventory Microservice configured in the Query Service?**

<details>
<summary>Answer</summary>

It is injected via MicroProfile Config using `@ConfigProperty`. The default value points to `localhost:9085`, but it can be overridden by setting the `INVENTORY_BASE_URI` environment variable — which is exactly what the integration test does when wiring the query container to MockServer:

```java
// InventoryClient.java
@Inject
@ConfigProperty(name = "INVENTORY_BASE_URI", defaultValue = "http://localhost:9085")
private String baseUri;
```

```java
// QueryServiceIT.java — override for containerised test
queryContainer.withEnv(
    "INVENTORY_BASE_URI",
    "http://mock-server:" + MockServerContainer.PORT);
```

This makes the URL environment-specific without recompiling the application.

</details>

---

**Q11. What happens if one of the reactive `getSystem()` calls fails or times out?**

<details>
<summary>Answer</summary>

The `subscribe()` error callback logs the exception and still decrements the `CountDownLatch`, so the Query Service does not hang waiting for a failed call. That host is simply omitted from the aggregated result.

```java
inventoryClient.getSystem(system)
               .subscribe(
                   p  -> {
                       if (p != null) systemLoads.updateValues(p);
                       remainingSystems.countDown();
                   },
                   ex -> {
                       ex.printStackTrace();          // log but don't rethrow
                       remainingSystems.countDown();  // unblock the latch regardless
                   }
               );
```

If _all_ calls take too long, the `await` timeout unblocks the main thread and returns whatever partial results were collected:

```java
remainingSystems.await(30, TimeUnit.SECONDS);  // give up after 30s
```

</details>
