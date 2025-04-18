package com.example;

import com.amazonaws.services.kinesisanalytics.runtime.KinesisAnalyticsRuntime;
import com.example.deserializer.JsonDeserializer;
import com.example.functions.PostgresSink;
import com.example.model.Message;
import com.google.gson.Gson;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Properties;

public class FlinkJob {

    private static final Logger LOG = LoggerFactory.getLogger(FlinkJob.class);

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        LOG.info("Starting Flink job...");

        Map<String, Properties> applicationProperties = KinesisAnalyticsRuntime.getApplicationProperties();
        Properties kafkaProperties = applicationProperties.get("InputKafkaProperties");

        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers(kafkaProperties.getProperty("bootstrap.servers"))
                .setTopics(kafkaProperties.getProperty("topic"))
                .setGroupId(kafkaProperties.getProperty("group.id"))
                .setDeserializer(KafkaRecordDeserializationSchema.valueOnly(new JsonDeserializer()))
                .build();

        DataStream<String> stream = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Kafka Source");

        DataStream<Tuple2<Integer, String>> processedStream = stream
                .map(value -> {
                    Message message = new Gson().fromJson(value, Message.class);
                    LOG.info("Message deserialized: id={}, message={}", message.getId(), message.getMessage());
                    return new Tuple2<>(message.getId(), message.getMessage());
                })
                .returns(TypeInformation.of(new TypeHint<Tuple2<Integer, String>>() {}))
                .map(tuple -> {
                    LOG.info("Processed Stream: id={}, message={}", tuple.f0, tuple.f1);
                    return tuple;
            })
            .returns(TypeInformation.of(new TypeHint<Tuple2<Integer, String>>() {}));

        processedStream.addSink(new PostgresSink());

        env.executeAsync("Kafka to PostgreSQL Flink Job");
    }
}