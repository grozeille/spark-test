import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.springframework.web.reactive.function.client.WebClient;

import java.io.*;
import java.nio.file.*;
import java.time.LocalDate;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

public class DailyApiProcessor {

    private static final int QUEUE_CAPACITY = 10;

    public static void main(String[] args) {
        // Parse options
        boolean clearCache = Boolean.parseBoolean(getArgument(args, "clearCache", "false"));
        boolean forceReload = Boolean.parseBoolean(getArgument(args, "forceReload", "false"));
        String date = getArgument(args, "date", LocalDate.now().toString());

        // Paths based on the date
        String cacheDir = "cache/" + date + "/";
        String outputDir = "output/" + date + "/";

        // Handle options
        if (clearCache) {
            clearDirectory(cacheDir);
            clearDirectory(outputDir);
        } else if (forceReload) {
            clearDirectory(outputDir);
        }

        // Initialize Spark
        SparkSession spark = SparkSession.builder()
                .appName("Daily API Data Processing")
                .master("local[*]")
                .getOrCreate();

        // Initialize WebClient
        WebClient webClient = WebClient.builder().baseUrl("https://api.example.com").build();

        // Queue for JSON data
        BlockingQueue<PageData> queue = new LinkedBlockingQueue<>(QUEUE_CAPACITY);

        // Atomic flag to indicate fetching completion
        AtomicBoolean isFetchingCompleted = new AtomicBoolean(false);

        // Resume page number
        int startPage = getLastProcessedPage(cacheDir);

        // Start data fetching thread
        Thread fetcherThread = new Thread(() -> fetchPages(webClient, cacheDir, startPage, queue, isFetchingCompleted));
        fetcherThread.start();

        // Start data processing thread
        Thread processorThread = new Thread(() -> processPages(spark, queue, outputDir, cacheDir, isFetchingCompleted, forceReload));
        processorThread.start();

        // Wait for threads to complete
        try {
            fetcherThread.join();
            processorThread.join();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Threads interrupted", e);
        }

        spark.stop();
    }

    private static void fetchPages(WebClient webClient, String cacheDir, int startPage, BlockingQueue<PageData> queue, AtomicBoolean isFetchingCompleted) {
        int page = startPage;
        try {
            while (true) {
                System.out.println("Fetching page: " + page);

                // Fetch data from the API
                String jsonData = webClient.get()
                        .uri(uriBuilder -> uriBuilder
                                .path("/data")
                                .queryParam("page", page)
                                .queryParam("size", 100)
                                .build())
                        .retrieve()
                        .bodyToMono(String.class)
                        .block();

                if (jsonData == null || jsonData.isEmpty()) {
                    System.out.println("No more data to fetch.");
                    break;
                }

                // Cache the page
                cachePage(page, jsonData, cacheDir);

                // Add to queue
                queue.put(new PageData(page, jsonData));

                page++;
            }
        } catch (Exception e) {
            System.err.println("Error fetching page: " + e.getMessage());
            e.printStackTrace();
        } finally {
            isFetchingCompleted.set(true);
        }
    }

    private static void processPages(SparkSession spark, BlockingQueue<PageData> queue, String outputDir, String cacheDir, AtomicBoolean isFetchingCompleted, boolean forceReload) {
        while (true) {
            try {
                // Take data from the queue
                PageData pageData = queue.poll();
                if (pageData == null) {
                    if (isFetchingCompleted.get()) break;
                    Thread.sleep(100); // Wait for more data
                    continue;
                }

                System.out.println("Processing page: " + pageData.getPage());

                // Skip already processed pages if not forcing reload
                if (!forceReload && Files.exists(Paths.get(outputDir + "page_" + pageData.getPage() + ".parquet"))) {
                    System.out.println("Page " + pageData.getPage() + " already processed. Skipping.");
                    continue;
                }

                // Load JSON into Spark and process
                Dataset<Row> rawDataset = spark.read().json(spark.createDataset(
                        java.util.Collections.singletonList(pageData.getJsonData()),
                        org.apache.spark.sql.Encoders.STRING()
                ));

                // Example transformation: select fields
                Dataset<Row> transformedDataset = rawDataset.select("id", "name", "timestamp");

                // Write to Parquet
                transformedDataset.write()
                        .mode("overwrite") // Overwrite the output for this page
                        .parquet(outputDir + "page_" + pageData.getPage() + ".parquet");

                // Mark the page as processed
                markPageAsProcessed(pageData.getPage(), cacheDir);
            } catch (Exception e) {
                System.err.println("Error processing page: " + e.getMessage());
                e.printStackTrace();
            }
        }
    }

    private static void cachePage(int page, String jsonData, String cacheDir) throws IOException {
        Files.createDirectories(Paths.get(cacheDir));
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(cacheDir + "page_" + page + ".json"))) {
            writer.write(jsonData);
        }
    }

    private static int getLastProcessedPage(String cacheDir) {
        File markerFile = new File(cacheDir + "last_processed_page.txt");
        if (!markerFile.exists()) return 0;

        try (BufferedReader reader = new BufferedReader(new FileReader(markerFile))) {
            return Integer.parseInt(reader.readLine());
        } catch (IOException e) {
            throw new RuntimeException("Failed to read last processed page", e);
        }
    }

    private static void markPageAsProcessed(int page, String cacheDir) {
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(cacheDir + "last_processed_page.txt"))) {
            writer.write(String.valueOf(page));
        } catch (IOException e) {
            throw new RuntimeException("Failed to mark page as processed", e);
        }
    }

    private static String getArgument(String[] args, String key, String defaultValue) {
        for (String arg : args) {
            if (arg.startsWith("--" + key + "=")) {
                return arg.split("=", 2)[1];
            }
        }
        return defaultValue;
    }

    private static void clearDirectory(String dir) {
        try {
            Files.walk(Paths.get(dir))
                    .sorted((a, b) -> b.compareTo(a)) // Delete files before directories
                    .map(Path::toFile)
                    .forEach(File::delete);
        } catch (IOException e) {
            System.err.println("Failed to clear directory: " + dir);
        }
    }

    private static class PageData {
        private final int page;
        private final String jsonData;

        public PageData(int page, String jsonData) {
            this.page = page;
            this.jsonData = jsonData;
        }

        public int getPage() {
            return page;
        }

        public String getJsonData() {
            return jsonData;
        }
    }
}