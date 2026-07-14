package io.openliberty.guides.query.client;

import java.util.List;
import java.util.Properties;

import jakarta.enterprise.context.RequestScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.client.ClientBuilder;
import jakarta.ws.rs.core.GenericType;
import jakarta.ws.rs.core.HttpHeaders;
import jakarta.ws.rs.core.MediaType;

import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.glassfish.jersey.client.rx.rxjava.RxObservableInvoker;
import org.glassfish.jersey.client.rx.rxjava.RxObservableInvokerProvider;

import rx.Observable;

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
                .get(new GenericType<List<String>>() { });
    }

    public Observable<Properties> getSystem(String hostname) {
        return ClientBuilder.newClient()
                .target(baseUri)
                .register(RxObservableInvokerProvider.class)
                .path("/inventory/systems")
                .path(hostname)
                .request()
                .header(HttpHeaders.CONTENT_TYPE,
                        MediaType.APPLICATION_JSON)
                .rx(RxObservableInvoker.class)
                .get(new GenericType<Properties>() { });
    }
}
