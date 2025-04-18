package com.example.functions;

import com.amazonaws.services.kinesisanalytics.runtime.KinesisAnalyticsRuntime;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Map;
import java.util.Properties;

public class PostgresSink extends RichSinkFunction<Tuple2<Integer, String>> {

    private static final Logger LOG = LoggerFactory.getLogger(PostgresSink.class);

    private transient Connection connection;
    private transient PreparedStatement preparedStatement;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        Map<String, Properties> applicationProperties = KinesisAnalyticsRuntime.getApplicationProperties();
        Properties postgresqlProperties = applicationProperties.get("OutputPostgreSQLProperties");

        String jdbcUrl = postgresqlProperties.getProperty("jdbc.url");
        String username = postgresqlProperties.getProperty("jdbc.username");
        String password = postgresqlProperties.getProperty("jdbc.password");

        try {
            connection = DriverManager.getConnection(jdbcUrl, username, password);
            LOG.info("Successfully connected to PostgreSQL at {}", jdbcUrl);
            String insertSQL = "INSERT INTO test.test_table (id, message) VALUES (?, ?)";
            preparedStatement = connection.prepareStatement(insertSQL);
        } catch (SQLException e) {
            LOG.error("Failed to connect to PostgreSQL at {}: {}", jdbcUrl, e.getMessage());
            throw new RuntimeException("Database connection failed", e);
        }
    }

    @Override
    public void invoke(Tuple2<Integer, String> value, Context context) throws Exception {

        LOG.info("Preparing to insert record into PostgreSQL: id={}, message={}", value.f0, value.f1);

        try {
            preparedStatement.setInt(1, value.f0);
            preparedStatement.setString(2, value.f1);
            preparedStatement.executeUpdate();
            LOG.info("Successfully inserted record: id={}, message={}", value.f0, value.f1);
        } catch (SQLException e) {
            LOG.error("Error executing insert statement for id={}, message={}", value.f0, value.f1, e);
        }
    }

    @Override
    public void close() throws Exception {
        if (preparedStatement != null) {
            preparedStatement.close();
            LOG.info("PreparedStatement closed.");
        }
        if (connection != null) {
            connection.close();
            LOG.info("Database connection closed.");
        }
        super.close();
    }
}