# flink-testcontainers-demo
A demo repository showcasing how to test Apache Flink jobs using Testcontainers. It provides a simple setup for running integration tests with Flink and Kafka in Docker containers, streamlining local testing and development without external dependencies.

## Project Structure

```bash
.
├── src/
│   ├── main/
│   │   ├── java/
│   │   │   └── com/
│   │   │       └── example/
│   │   │           ├── FlinkJob.java            # Flink Job entry point
│   │   │           ├── functions/
│   │   │           │   └── PostgresSink.java    # Custom sink for PostgreSQL
│   │   │           └── model/
│   │   │               └── Message.java          # Model for Kafka message
│   ├── test/
│   │   └── java/
│   │       └── com/
│   │           └── example/
│   │               └── FlinkJobContainerTest.java  # Test class for Flink job using Testcontainers
├── resources/
│   ├── config.properties                       # Configuration file with Kafka and PostgreSQL settings
│   ├── flink-conf.yaml                        # Flink configuration file
│   └── application-properties.json            # JSON containing Kafka and PostgreSQL configurations
└── pom.xml                                    # Maven build file
```
## Requirements

* **Apache Kafka** - Used for message streaming.
* **Apache Flink** - Used for processing the stream and writing the results to PostgreSQL.
* **PostgreSQL** - Used as the sink for the processed data.
* **Testcontainers** - Used for running Kafka, PostgreSQL and Flink containers for testing.
* **Maven** - Used for building the project and managing dependencies.

## Setup and Configuration

1. Clone the Repository:
```bash
git clone git@github.com:pavanraotk/flink-testcontainers-demo.git
cd flink-testcontainers-demo
```
2. Build the project
```bash
mvn clean package
```
3. Running tests
```bash
mvn test
```

## Code Breakdown

* **FlinkJob.java**: The entry point of the Flink job. It sets up the Kafka source and defines the stream processing pipeline. After processing, it writes the data to PostgreSQL using the custom `PostgresSink`.
* **PostgresSink.java**: A custom RichSinkFunction to handle inserting processed records into PostgreSQL. It opens a connection to PostgreSQL, executes INSERT statements, and logs the insertion process.
* **Message.java**: A simple Java class representing the structure of the messages being processed. It includes fields for `id`, `name`, and `timestamp`.
* **FlinkJobContainerTest.java**: A test class that uses Testcontainers to spin up Kafka and PostgreSQL containers. It sends test messages to Kafka, runs the Flink job, and verifies that the data is correctly inserted into PostgreSQL.

## Conclusion

This project demonstrates how to use Apache Flink for stream processing and PostgreSQL for data storage. By leveraging Testcontainers, you can easily test the entire pipeline locally with Kafka and PostgreSQL containers.