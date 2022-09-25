package io.example.sparkconsumer.config;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.*;

@Configuration
public class SparkStreamContext {
    private final JavaStreamingContext sparkConf;

    @Value("${kafka.boostrap.server}")
    private String bootstrapServer;

    @Value("${kafka.group.id}")
    private String groupId;

    @Value("${kafka.topic}")
    private String topic;

    @Autowired
    public SparkStreamContext(JavaStreamingContext sparkConf) {
        this.sparkConf = sparkConf;
    }

    public Map<String, Object> getParams(){
        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", bootstrapServer);
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", groupId);
        kafkaParams.put("auto.offset.reset", "earliest");
        return kafkaParams;
    }

    @Bean
    public JavaInputDStream<ConsumerRecord<String, String>> getJavaInputDStream(){
        Collection<String> topics = List.of(topic);
        return KafkaUtils.createDirectStream(
                sparkConf,
                LocationStrategies.PreferConsistent(),
                ConsumerStrategies.Subscribe(topics, getParams())
        );
    }

}
