import org.springframework.security.oauth2.client.registration.ClientRegistration;
import org.springframework.security.oauth2.client.registration.ClientRegistrations;
import org.springframework.security.oauth2.client.registration.ReactiveClientRegistrationRepository;
import org.springframework.security.oauth2.client.web.reactive.function.client.ServletOAuth2AuthorizedClientExchangeFilterFunction;
import org.springframework.web.reactive.function.client.WebClient;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class WebClientFactory {

    // Cache to store WebClients by API ID
    private final Map<String, WebClient> webClientCache = new ConcurrentHashMap<>();

    /**
     * Creates or retrieves a WebClient configured for OAuth2 client credentials flow.
     *
     * @param apiId     Unique identifier for the API
     * @param clientId  OAuth2 client ID
     * @param clientSecret OAuth2 client secret
     * @param scope     Scope for the API
     * @param tokenUri  OAuth2 token URI
     * @return A configured WebClient instance
     */
    public WebClient getOrCreateWebClient(String apiId, String clientId, String clientSecret, String scope, String tokenUri) {
        return webClientCache.computeIfAbsent(apiId, id -> createWebClient(clientId, clientSecret, scope, tokenUri));
    }

    /**
     * Creates a new WebClient configured for OAuth2 client credentials flow.
     *
     * @param clientId     OAuth2 client ID
     * @param clientSecret OAuth2 client secret
     * @param scope        Scope for the API
     * @param tokenUri     OAuth2 token URI
     * @return A configured WebClient instance
     */
    private WebClient createWebClient(String clientId, String clientSecret, String scope, String tokenUri) {
        // Create a ClientRegistration programmatically
        ClientRegistration clientRegistration = ClientRegistration.withRegistrationId("dynamic-client")
                .clientId(clientId)
                .clientSecret(clientSecret)
                .scope(scope)
                .tokenUri(tokenUri)
                .authorizationGrantType(org.springframework.security.oauth2.core.AuthorizationGrantType.CLIENT_CREDENTIALS)
                .build();

        // Reactive repository
        ReactiveClientRegistrationRepository clientRegistrationRepository =
                new org.springframework.security.oauth2.client.registration.InMemoryReactiveClientRegistrationRepository(clientRegistration);

        // Configure OAuth2 filter function
        ServletOAuth2AuthorizedClientExchangeFilterFunction oauth2FilterFunction =
                new ServletOAuth2AuthorizedClientExchangeFilterFunction(
                        clientRegistrationRepository, new org.springframework.security.oauth2.client.web.DefaultReactiveOAuth2AuthorizedClientManager());

        oauth2FilterFunction.setDefaultClientRegistrationId(clientRegistration.getRegistrationId());

        // Initialize WebClient with OAuth2 support
        return WebClient.builder()
                .apply(oauth2FilterFunction.oauth2Configuration())
                .build();
    }
}