package org.example;

import org.apache.spark.sql.Dataset;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class BlacklistWordFilter {
    private Connection blacklistWordDatabase = connectToDatabase();

    public Dataset<String> filterWords(Dataset<String> original) {
        // TODO remove all words we have in the blacklist stored in the DB
        connectToDatabase();

        return original;
    }

    private Connection connectToDatabase() {
        try {
            Class.forName("org.h2.Driver");
            String jdbcUrl = "jdbc:h2:mem:test;DB_CLOSE_DELAY=-1"; // In-memory H2 database
            String username = "sa";
            String password = "";

            return DriverManager.getConnection(jdbcUrl, username, password);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
