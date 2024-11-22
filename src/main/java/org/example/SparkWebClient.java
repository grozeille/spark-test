import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.List;

public class SparkDataProcessor {

    public static void main(String[] args) {
        // Initialize Spark
        SparkSession spark = SparkSession.builder()
                .appName("Paginated API Data Processing")
                .master("local[*]")
                .getOrCreate();

        // Initialize API fetcher
        ApiFetcher apiFetcher = new ApiFetcher("https://api.example.com");

        // Fetch paginated data
        List<String> paginatedData = apiFetcher.fetchPaginatedData("/data", 100);

        // Load data into Spark
        Dataset<Row> rawDataset = spark.read().json(spark.createDataset(paginatedData, spark.implicits().stringEncoder()));

        // Transformation example: Select specific fields
        Dataset<Row> transformedDataset = rawDataset.select("id", "name", "timestamp");

        // Optionally call another API for details based on `id`
        Dataset<Row> enrichedDataset = transformedDataset.map(row -> {
            String id = row.getAs("id");
            String details = fetchDetailsFromApi(id); // Call another API for details
            return RowFactory.create(id, row.getAs("name"), details, row.getAs("timestamp"));
        }, Encoders.row(StructType.create(
                Arrays.asList(
                        new StructField("id", DataTypes.StringType, true, Metadata.empty()),
                        new StructField("name", DataTypes.StringType, true, Metadata.empty()),
                        new StructField("details", DataTypes.StringType, true, Metadata.empty()),
                        new StructField("timestamp", DataTypes.TimestampType, true, Metadata.empty())
                )
        )));

        // Store data as Parquet
        enrichedDataset.write().parquet("output/path/data.parquet");

        spark.stop();
    }

    private static String fetchDetailsFromApi(String id) {
        // Use WebClient or other HTTP client to fetch details
        WebClient webClient = WebClient.create("https://api.example.com");
        return webClient.get()
                .uri("/details/{id}", id)
                .retrieve()
                .bodyToMono(String.class)
                .block();
    }
}