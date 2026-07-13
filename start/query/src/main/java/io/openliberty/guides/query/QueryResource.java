package io.openliberty.guides.query;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;

import io.openliberty.guides.query.client.InventoryClient;

@ApplicationScoped
@Path("/query")
public class QueryResource {

    @Inject
    private InventoryClient inventoryClient;

    @GET
    @Path("/systemLoad")
    @Produces(MediaType.APPLICATION_JSON)
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

        // Wait for all remaining systems to be checked
        try {
            remainingSystems.await(30, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        return systemLoads.getValues();
    }

    private class Holder {
        private volatile Map<String, Properties> values;

        Holder() {
            this.values = new ConcurrentHashMap<String, Properties>();
            init();
        }

        public Map<String, Properties> getValues() {
            return this.values;
        }

        public void updateValues(Properties p) {
            final BigDecimal load = (BigDecimal) p.get("systemLoad");

            this.values.computeIfPresent("lowest", (key, curr_val) -> {
                BigDecimal lowest = (BigDecimal) curr_val.get("systemLoad");
                return load.compareTo(lowest) < 0 ? p : curr_val;
            });
            this.values.computeIfPresent("highest", (key, curr_val) -> {
                BigDecimal highest = (BigDecimal) curr_val.get("systemLoad");
                return load.compareTo(highest) > 0 ? p : curr_val;
            });
        }

        private void init() {
            // Initialize highest and lowest values
            this.values.put("highest", new Properties());
            this.values.put("lowest", new Properties());
            this.values.get("highest").put("hostname", "temp_max");
            this.values.get("lowest").put("hostname", "temp_min");
            this.values.get("highest")
                    .put("systemLoad", new BigDecimal(Double.MIN_VALUE));
            this.values.get("lowest")
                    .put("systemLoad", new BigDecimal(Double.MAX_VALUE));
        }
    }
}
