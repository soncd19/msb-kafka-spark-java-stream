package com.msb.stream.connector;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

public class DatabaseConnector implements Serializable {
    private static final Logger logger = LoggerFactory.getLogger(DatabaseConnector.class);
    private Connection connection = null;

    private final Properties properties;

    public DatabaseConnector(Properties properties) {
        this.properties = properties;
    }

    public Connection init() throws SQLException {
        HikariConfig config = new HikariConfig();

        config.setJdbcUrl(properties.getProperty("dataSource.uri"));
        config.setUsername(properties.getProperty("dataSource.user"));
        config.setPassword(properties.getProperty("dataSource.password"));
        config.setDriverClassName("oracle.jdbc.driver.OracleDriver");
        config.setMaximumPoolSize(100);
        config.addDataSourceProperty("cachePrepStmts", "true");
        config.addDataSourceProperty("prepStmtCacheSize", "250");
        config.addDataSourceProperty("prepStmtCacheSqlLimit", "2048");
        HikariDataSource ds = new HikariDataSource(config);
        logger.info("created database connector");
        return ds.getConnection();
    }

    public Connection getConnection() throws SQLException {
        if (connection == null) {
            connection = init();
        }
        return connection;
    }

    public void close() throws SQLException {
        if (connection != null) {
            connection.close();
        }
    }
}
