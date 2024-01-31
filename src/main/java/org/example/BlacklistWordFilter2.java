package org.example;

import org.apache.spark.sql.Dataset;

import java.io.Serializable;
import java.sql.*;

public class BlacklistWordFilter2 implements Serializable {
    private transient Connection blacklistWordDatabase = connectToDatabase();

    public Dataset<String> filterWords(Dataset<String> original) {
        // remove all words we have in the blacklist stored in the DB
        return original.filter((String s) -> {
            String query = "select count(*) as count from blacklist where word = ?";
            try(PreparedStatement ps = blacklistWordDatabase.prepareStatement(query)) {
                ps.setString(1, s);
                try(ResultSet rs = ps.executeQuery()) {
                    rs.next();
                    int count = rs.getInt("count");
                    return count != 0;
                }
            }
        });
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
