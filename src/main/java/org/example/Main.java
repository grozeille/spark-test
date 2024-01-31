package org.example;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;

import java.io.IOException;

@Slf4j
public class Main {

    public static final String OVERWRITE = "overwrite";

    public static void main(String[] args) {
        String logFile = "src/main/resources/data.txt";
        SparkSession spark = SparkSession.builder()
                .appName("Simple Application")
                .config("spark.sql.adaptive.enabled", "false")
                .config("spark.sql.adaptive.coalescePartitions.enabled", "false")
                .master("local[*]")
                .getOrCreate();

        // what happen if we don't use cache()
        Dataset<String> logData = spark.read().textFile(logFile).cache();
        System.out.printf("Number of partitions : %d%n", logData.javaRDD().getNumPartitions());
        //log.info(logData.part);
        long numAs = logData.filter((String s) -> s.contains("a")).count();
        long numBs = logData.filter((String s) -> s.contains("b")).count();
        System.out.println("Lines with a: " + numAs + ", lines with b: " + numBs);

        // what do you think about this code
        int countA = 0;
        for(String line : logData.collectAsList()) {
            if(line.contains("a")) {
                countA++;
            }
        }

        // why do we have an exception ?
        logData = new BlacklistWordFilter().filterWords(logData);

        // what's the purpose of the coalesce ?
        logData.filter((String s) -> s.contains("b"))
               .coalesce(1)
               .write().mode(OVERWRITE).parquet("target/lines_with_b");

        System.out.println("Press a key to exit...");
        try {
            System.in.read();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        spark.stop();
    }
}
