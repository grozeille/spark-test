import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.springframework.web.reactive.function.client.WebClient;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.stream.IntStream;

public class IncrementalSparkProcessor {

    private static final String CACHE_DIR = "cache/";

    public static void main(String[] args) {
        // Initialize Spark
        SparkSession spark = SparkSession.builder()
                .appName("Incremental API Data Processing")
                .master("local[*]")
                .getOrCreate();

        // Initialize WebClient
        WebClient webClient = WebClient.builder().baseUrl("https://api.example.com").build();

        // Define API parameters
        String endpoint = "/data";
        int pageSize = 100;

        // Resume from the last processed page
        int startPage = getLastProcessedPage() + 1;

        // Fetch, process, and cache pages
        IntStream.iterate(startPage, page -> page + 1)
                .forEach(page -> {
                    try {
                        System.out.println("Processing page: " + page);

                        // Fetch the current page
                        String jsonData = fetchPage(webClient, endpoint, page, pageSize);

                        if (jsonData == null || jsonData.isEmpty()) {
                            System.out.println("No more data to process.");
                            return;
                        }

                        // Cache the page
                        cachePage(page, jsonData);

                        // Process the page in Spark
                        processPageInSpark(spark, jsonData);

                        // Mark page as processed
                        markPageAsProcessed(page);

                    } catch (Exception e) {
                        System.err.println("Error processing page " + page + ": " + e.getMessage());
                        e.printStackTrace();
                        // Stop processing on failure
                        throw new RuntimeException(e);
                    }
                });

        spark.stop();
    }

    // Fetch a single page from the API
    private static String fetchPage(WebClient webClient, String endpoint, int page, int pageSize) {
        return webClient.get()
                .uri(uriBuilder -> uriBuilder
                        .path(endpoint)
                        .queryParam("page", page)
                        .queryParam("size", pageSize)
                        .build())
                .retrieve()
                .bodyToMono(String.class)
                .block();
    }

    // Cache the JSON data for a page
    private static void cachePage(int page, String jsonData) throws IOException {
        Files.createDirectories(Paths.get(CACHE_DIR));
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(CACHE_DIR + "page_" + page + ".json"))) {
            writer.write(jsonData);
        }
    }

    // Process a single page of data in Spark
    private static void processPageInSpark(SparkSession spark, String jsonData) {
        // Load JSON data into Spark Dataset
        Dataset<Row> rawDataset = spark.read().json(spark.createDataset(
                java.util.Collections.singletonList(jsonData),
                org.apache.spark.sql.Encoders.STRING()
        ));

        // Transformation example: Select specific fields
        Dataset<Row> transformedDataset = rawDataset.select("id", "name", "timestamp");

        // Save transformed data as Parquet
        transformedDataset.write()
                .mode("append")  // Append mode to add new data incrementally
                .parquet("output/data.parquet");
    }

    // Retrieve the last processed page from a marker file
    private static int getLastProcessedPage() {
        File markerFile = new File(CACHE_DIR + "last_processed_page.txt");
        if (!markerFile.exists()) return 0;

        try (BufferedReader reader = new BufferedReader(new FileReader(markerFile))) {
            return Integer.parseInt(reader.readLine());
        } catch (IOException e) {
            throw new RuntimeException("Failed to read last processed page", e);
        }
    }

    // Mark a page as processed by updating the marker file
    private static void markPageAsProcessed(int page) {
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(CACHE_DIR + "last_processed_page.txt"))) {
            writer.write(String.valueOf(page));
        } catch (IOException e) {
            throw new RuntimeException("Failed to mark page as processed", e);
        }
    }
}