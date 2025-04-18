package com.example;

import com.example.model.Message;
import com.github.dockerjava.api.model.ExposedPort;
import com.github.dockerjava.api.model.PortBinding;
import com.github.dockerjava.api.model.Ports;
import com.google.gson.Gson;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.Container.ExecResult;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.shaded.org.awaitility.Awaitility;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;
import static org.junit.jupiter.api.Assertions.assertEquals;

@Testcontainers
class FlinkJobContainerTest {
    private static final Network network = Network.newNetwork();
    private static final String KAFKA_TOPIC = "test-topic";
    private static final String KAFKA_TEST_GROUP_ID = "test-group";
    private static final String KAFKA_GROUP_ID = "group-id";
    private static final Gson gson = new Gson();

    @Container
    private static final GenericContainer<?> kafka = new GenericContainer<>("bitnami/kafka:latest")
            .withNetwork(network)
            .withNetworkAliases("kafka")
            .withCreateContainerCmdModifier(cmd -> cmd
                    .withPortBindings(new PortBinding(Ports.Binding.bindPort(9092), new ExposedPort(9092)),
                            new PortBinding(Ports.Binding.bindPort(9094), new ExposedPort(9094))))
            .withEnv("KAFKA_CFG_NODE_ID", "1")
            .withEnv("KAFKA_CFG_PROCESS_ROLES", "broker,controller")
            .withEnv("KAFKA_CFG_CONTROLLER_QUORUM_VOTERS", "1@kafka:9093")
            .withEnv("KAFKA_CFG_CONTROLLER_LISTENER_NAMES", "CONTROLLER")
            .withEnv("KAFKA_CFG_LISTENERS", "INTERNAL://kafka:9092,EXTERNAL://kafka:9094,CONTROLLER://kafka:9093")
            .withEnv("KAFKA_CFG_ADVERTISED_LISTENERS", "INTERNAL://kafka:9092,EXTERNAL://localhost:9094")
            .withEnv("KAFKA_CFG_LISTENER_SECURITY_PROTOCOL_MAP", "INTERNAL:PLAINTEXT,EXTERNAL:PLAINTEXT,CONTROLLER:PLAINTEXT")
            .withEnv("KAFKA_CFG_INTER_BROKER_LISTENER_NAME", "INTERNAL")
            .withEnv("ALLOW_PLAINTEXT_LISTENER", "yes")
            .withEnv("KAFKA_CFG_LOG_DIRS", "/tmp/kafka-logs")
            .withEnv("KAFKA_CFG_AUTO_CREATE_TOPICS_ENABLE", "false")
            .withEnv("KAFKA_CREATE_TOPICS", KAFKA_TOPIC + ":1:1")
            .withExposedPorts(9092, 9094)
            .waitingFor(Wait.forListeningPort().withStartupTimeout(Duration.ofSeconds(30)));

    @Container
    private static final PostgreSQLContainer<?> postgresContainer = new PostgreSQLContainer<>("postgres:latest")
            .withNetwork(network)
            .withNetworkAliases("postgres")
            .withDatabaseName("testDb")
            .withUsername("user")
            .withPassword("password");

    private static String absoluteJarFilePath;
    private File propertiesJsonFile;

    @BeforeAll
    static void setupBeforeAll() throws IOException, InterruptedException {
        Files.copy(Path.of("src/test/resources/config.properties"),
                Path.of("src/main/resources/config.properties"),
                REPLACE_EXISTING);

        ProcessBuilder processBuilder = new ProcessBuilder("mvn", "package", "-DskipTests=true");

        Process mvnPackageProcess = processBuilder.inheritIO().start();
        int mvnPackageExitCode = mvnPackageProcess.waitFor();

        if (mvnPackageExitCode != 0) {
            throw new RuntimeException("Maven package failed with exit code " + mvnPackageExitCode);
        }

        File jarFile = new File("target/flink-testcontainers-demo-1.0-SNAPSHOT.jar");
        if (!jarFile.exists()) {
            throw new RuntimeException("JAR file not found: " + jarFile.getAbsolutePath());
        }
        absoluteJarFilePath = jarFile.getAbsolutePath();
    }

    @BeforeEach
    void setup() {
        createTopic();
        checkIfTopicExists();
        produceTestMessages();
        checkIfMessagesExistInTopic();
        createMessagesTable();
        propertiesJsonFile = constructPropertiesJsonFile();
    }

    @Test
    void testFlinkJob() throws IOException, InterruptedException {
        GenericContainer<?> flinkJobManager = constructAndStartFlinkJobManager();
        constructAndStartFlinkTaskManager(flinkJobManager);

        flinkJobManager.copyFileToContainer(MountableFile.forHostPath(absoluteJarFilePath),
                "/opt/flink/flink-testcontainers-demo-1.0-SNAPSHOT.jar");

        ExecResult execResult = flinkJobManager.execInContainer("flink",
                "run",
                "-c",
                "com.example.FlinkJob",
                "/opt/flink/flink-testcontainers-demo-1.0-SNAPSHOT.jar");

        assertEquals(0, execResult.getExitCode(),
                "Flink job failed with exit code: " + execResult.getExitCode());

        Awaitility.await()
                .atMost(60, TimeUnit.SECONDS)
                .until(this::checkRowCountInTable);
    }

    private boolean checkRowCountInTable() {
        try (Connection connection = postgresContainer.createConnection("")) {
            String query = "SELECT COUNT(*) FROM test.test_table";
            try (Statement stmt = connection.createStatement();
                 ResultSet resultSet = stmt.executeQuery(query)) {
                if (resultSet.next()) {
                    return resultSet.getInt(1) == 10;
                }
            }
        } catch (Exception e) {
            return false;
        }
        return false;
    }

    private void createMessagesTable() {
        try (Connection connection = postgresContainer.createConnection("")) {
            String createSchemaSQL = "CREATE SCHEMA IF NOT EXISTS test;";
            connection.createStatement().execute(createSchemaSQL);

            String createTableSQL = "CREATE TABLE IF NOT EXISTS test.test_table (" +
                    "id INTEGER PRIMARY KEY, " +
                    "message VARCHAR(255) NOT NULL" +
                    ");";
            connection.createStatement().execute(createTableSQL);
        } catch (Exception e) {
            throw new RuntimeException("Error creating messages table", e);
        }
    }

    private void constructAndStartFlinkTaskManager(GenericContainer<?> flinkJobManager) {
        GenericContainer<?> taskManagerContainer = new GenericContainer<>(DockerImageName.parse("arm64v8/flink:1.20.1-java11"))
                .withCopyFileToContainer(MountableFile.forHostPath(propertiesJsonFile.getAbsolutePath()),
                        "/opt/flink/conf/application-properties.json")
                .withCopyFileToContainer(MountableFile.forHostPath(Paths.get("src/test/resources/flink-conf.yaml").toAbsolutePath()),
                        "/opt/flink/conf/flink-conf.yaml")
                .withNetworkAliases("taskmanager")
                .withNetwork(network)
                .dependsOn(flinkJobManager)
                .withCommand("taskmanager")
                .withEnv("JOB_MANAGER_RPC_ADDRESS", "jobmanager")
                .waitingFor(Wait.forLogMessage(".*Successful registration at resource manager.*", 1)
                        .withStartupTimeout(Duration.ofSeconds(120)));
        taskManagerContainer.start();
    }

    private GenericContainer<?> constructAndStartFlinkJobManager() {
        try (GenericContainer<?> flinkJobManager = new GenericContainer<>(DockerImageName.parse("arm64v8/flink:1.20.1-java11"))
                .withCopyFileToContainer(MountableFile.forHostPath(propertiesJsonFile.getAbsolutePath()),
                        "/opt/flink/conf/application-properties.json")
                .withNetwork(network)
                .withNetworkAliases("jobmanager")
                .withCommand("jobmanager")
                .withExposedPorts(8081)
                .withEnv("JOB_MANAGER_RPC_ADDRESS", "jobmanager")
                .waitingFor(Wait.forHttp("/")
                        .forPort(8081)
                        .withStartupTimeout(Duration.ofSeconds(120)))) {
            flinkJobManager.start();
            return flinkJobManager;
        } catch (Exception e) {
            throw new RuntimeException("Error starting Flink JobManager", e);
        }
    }

    private File constructPropertiesJsonFile() {
        List<Map<String, Object>> propertyGroups = new ArrayList<>();

        Map<String, Object> kafkaPropertiesGroup = new HashMap<>();

        Map<String, String> kafkaProperties = new HashMap<>();
        kafkaProperties.put("bootstrap.servers", "kafka:9092");
        kafkaProperties.put("topic", KAFKA_TOPIC);
        kafkaProperties.put("group.id", KAFKA_GROUP_ID);

        kafkaPropertiesGroup.put("PropertyGroupId", "InputKafkaProperties");
        kafkaPropertiesGroup.put("PropertyMap", kafkaProperties);

        propertyGroups.add(kafkaPropertiesGroup);

        Map<String, Object> postgresqlPropertiesGroup = new HashMap<>();
        Map<String, String> postgresqlProperties = new HashMap<>();
        postgresqlProperties.put("jdbc.url", "jdbc:postgresql://postgres:5432/testDb");
        postgresqlProperties.put("jdbc.username", "user");
        postgresqlProperties.put("jdbc.password", "password");

        postgresqlPropertiesGroup.put("PropertyGroupId", "OutputPostgreSQLProperties");
        postgresqlPropertiesGroup.put("PropertyMap", postgresqlProperties);
        propertyGroups.add(postgresqlPropertiesGroup);

        String json = gson.toJson(propertyGroups);
        File applicationPropertiesJsonFile = new File("application-properties.json");
        try(FileWriter writer = new FileWriter(applicationPropertiesJsonFile)) {
            writer.write(json);
        } catch (IOException e) {
            throw new RuntimeException("Error writing JSON to file", e);
        }
        return applicationPropertiesJsonFile;
    }

    private void checkIfMessagesExistInTopic() {
        Properties consumerProps = new Properties();
        consumerProps.put("bootstrap.servers", "localhost:9094");
        consumerProps.put("group.id", KAFKA_TEST_GROUP_ID);
        consumerProps.put("key.deserializer", StringDeserializer.class.getName());
        consumerProps.put("value.deserializer", StringDeserializer.class.getName());

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps)) {
            List<PartitionInfo> partitionInfoList = consumer.partitionsFor(KAFKA_TOPIC);
            List<TopicPartition> topicPartitions = partitionInfoList.stream()
                    .map(partition -> new TopicPartition(partition.topic(), partition.partition()))
                    .collect(Collectors.toList());
            consumer.assign(topicPartitions);
            consumer.seekToEnd(topicPartitions);

            long endOffset = 0;
            for (TopicPartition partition : topicPartitions) {
                endOffset += consumer.endOffsets(Collections.singleton(partition))
                        .get(partition);
            }
            if (endOffset != 10) {
                throw new RuntimeException("Expected 10 messages in topic " + KAFKA_TOPIC + ", but found " + endOffset);
            }
        } catch (Exception e) {
            throw new RuntimeException("Error checking if messages exist in topic", e);
        }
    }

    private void produceTestMessages() {
        Properties producerProps = new Properties();
        producerProps.put("bootstrap.servers", "localhost:9094");
        producerProps.put("key.serializer", StringSerializer.class.getName());
        producerProps.put("value.serializer", StringSerializer.class.getName());

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps)) {
            for (int i = 0; i < 10; i++) {
                Message message = new Message(i, "test-message-" + i);
                String jsonMessage = gson.toJson(message);
                producer.send(new ProducerRecord<>(KAFKA_TOPIC, Integer.toString(i), jsonMessage));
            }
        } catch (Exception e) {
            throw new RuntimeException("Error producing test messages", e);
        }
    }

    private void checkIfTopicExists() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9094");
        try (AdminClient adminClient = AdminClient.create(props)) {
            boolean exists = adminClient.listTopics().names().get().contains(KAFKA_TOPIC);
            if (!exists) {
                throw new RuntimeException("Topic " + KAFKA_TOPIC + " does not exist.");
            }
        } catch (Exception e) {
            throw new RuntimeException("Error checking if topics exist", e);
        }
    }

    private void createTopic() {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9094");

        try (AdminClient adminClient = AdminClient.create(properties)) {
            NewTopic newTopic = new NewTopic(KAFKA_TOPIC, 1, (short) 1);
            adminClient.createTopics(Collections.singletonList(newTopic))
                    .all()
                    .get();
        } catch (Exception e) {
            if (e instanceof TopicExistsException) {
                System.out.println("Topic " + KAFKA_TOPIC + " already exists.");
            } else {
                throw new RuntimeException("Error creating topic: " + e.getMessage(), e);
            }
        }
    }
}