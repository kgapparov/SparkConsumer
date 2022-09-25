package io.example.sparkconsumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;


@SpringBootApplication
public class BdtSparkStreamConsumerApplication {
    private static final Logger log = LoggerFactory.getLogger(BdtSparkStreamConsumerApplication.class.getName());

    public static void main(String[] args) {
        SpringApplication.run(BdtSparkStreamConsumerApplication.class, args);
    }

    @Bean
    public CommandLineRunner renner(JavaStreamingContext javaStreamingContext, JavaInputDStream<ConsumerRecord<String, String>> javaInputDStream) {
        return args -> {
            javaInputDStream.foreachRDD( rdd -> {
                    rdd.foreachPartition(
                            consumerRecords -> {
                                while (consumerRecords.hasNext()) {
                                    ConsumerRecord<String, String> record = consumerRecords.next();
                                   log.info("key " + record.key() +  " Value " + record.value());
                                }
                            }
                    );
                }
            );
            javaStreamingContext.start();
            javaStreamingContext.awaitTermination();
        };
    }
}
