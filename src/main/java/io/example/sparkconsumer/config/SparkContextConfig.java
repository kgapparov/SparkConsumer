package io.example.sparkconsumer.config;


import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class SparkContextConfig {
    @Value("${spark.context.master}")
    private String server;
    @Value("${spark.context.app.name}")
    private String appName;

    @Bean
    public JavaStreamingContext getContext(){
        return new JavaStreamingContext(server, appName, Durations.seconds(20));
    }
}
