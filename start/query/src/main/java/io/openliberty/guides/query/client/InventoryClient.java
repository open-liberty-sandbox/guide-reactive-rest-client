package io.openliberty.guides.query.client;

import jakarta.enterprise.context.RequestScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.client.ClientBuilder;
import jakarta.ws.rs.core.GenericType;
import jakarta.ws.rs.core.HttpHeaders;
import jakarta.ws.rs.core.MediaType;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.CompletionStage;

@RequestScoped
public class InventoryClient {

    @Inject
    @ConfigProperty(name = "INVENTORY_BASE_URI", defaultValue = "http://localhost:9085")
    private String baseUri;


    public List<String> getSystems() {
        return ClientBuilder.newClient()
                .target(baseUri)
                .path("/inventory/systems")
                .request()
                .header(HttpHeaders.CONTENT_TYPE,
                        MediaType.APPLICATION_JSON)
                .get(new GenericType<List<String>>() {
                });
    }

    public CompletionStage<Properties> getSystem(String hostname) {
        return ClientBuilder.newClient()
                .target(baseUri)
                .path("/inventory/systems")
                .path(hostname)
                .request()
                .header(HttpHeaders.CONTENT_TYPE,
                        MediaType.APPLICATION_JSON)
                .rx()
                .get(Properties.class);
    }
}
