package org.ldbcouncil.finbench.impls.tigergraph;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.ldbcouncil.finbench.driver.DbConnectionState;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;

public class TigerGraphDbConnectionState extends DbConnectionState {
    static Logger logger = LogManager.getLogger("TigerGraphDbConnectionState");
    private static HikariDataSource dataSource; // Singleton instance

    public TigerGraphDbConnectionState(Map<String, String> properties) throws IOException {
        if (dataSource == null) {
            initializeDataSource(properties);
        }
    }

    private void initializeDataSource(Map<String, String> properties) {
        String ipAddr = properties.get("ipAddr");
        String port = properties.get("port");
        String user = properties.get("user");
        String pass = properties.get("pass");
        String graph = properties.get("graph");
        int maxPoolSize = Integer.parseInt(properties.get("maxPoolSize"));

        HikariConfig config = new HikariConfig();
        StringBuilder sb = new StringBuilder();
        sb.append("jdbc:tg:http://").append(ipAddr).append(":").append(port);
        config.setJdbcUrl(sb.toString());
        config.setDriverClassName("com.tigergraph.jdbc.Driver");
        config.setUsername(user);
        config.setPassword(pass);
        config.addDataSourceProperty("graph", graph);
        config.addDataSourceProperty("debug", 0);
        config.setMaximumPoolSize(maxPoolSize);

        dataSource = new HikariDataSource(config);
    }

    public Connection getPooledConn() throws SQLException {
        return dataSource.getConnection();
    }

    @Override
    public void close() throws IOException {
        // Close the data source after all connections have been closed
        if (dataSource != null) {
            dataSource.close();
        }
    }
}