public class Main {
    public static void main(String[] args) {
        WebClientFactory webClientFactory = new WebClientFactory();

        // Parameters for API 1
        String apiId1 = "api1";
        String clientId1 = "client-id-1";
        String clientSecret1 = "client-secret-1";
        String scope1 = "scope1";
        String tokenUri1 = "https://auth.example.com/oauth/token";

        // Create or get WebClient for API 1
        WebClient webClient1 = webClientFactory.getOrCreateWebClient(apiId1, clientId1, clientSecret1, scope1, tokenUri1);

        // Call the API
        String apiUrl = "https://api.example.com/resource";
        String response = webClient1.get()
                .uri(apiUrl)
                .retrieve()
                .bodyToMono(String.class)
                .block();

        System.out.println("Response from API 1: " + response);

        // Parameters for API 2 (different client)
        String apiId2 = "api2";
        String clientId2 = "client-id-2";
        String clientSecret2 = "client-secret-2";
        String scope2 = "scope2";
        String tokenUri2 = "https://auth.another.com/oauth/token";

        // Create or get WebClient for API 2
        WebClient webClient2 = webClientFactory.getOrCreateWebClient(apiId2, clientId2, clientSecret2, scope2, tokenUri2);

        // Call the second API
        String apiUrl2 = "https://api.another.com/resource";
        String response2 = webClient2.get()
                .uri(apiUrl2)
                .retrieve()
                .bodyToMono(String.class)
                .block();

        System.out.println("Response from API 2: " + response2);
    }
}