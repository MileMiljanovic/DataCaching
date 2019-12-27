package com.devoteam.kafka.kafkaproducer;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Autowired;

@Configuration
public class KafkaConfiguration {
	
	@Value("${kafka.url}")
    private String url;
	
	@Autowired
    private KafkaProperties kafkaProperties;
	
	@Bean
	public Map<String, Object> producerFactoryConfig() {
		
		Map<String, Object> config = new HashMap<>(kafkaProperties.buildProducerProperties());
		
		config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, url);
		config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
		config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class); 
		
		return config;
		
	}
	
	@Bean
    public ProducerFactory<String, String> producerFactory() {
        return new DefaultKafkaProducerFactory<>(producerFactoryConfig());
    }
	
	@Bean
    public KafkaTemplate<String, String> kafkaTemplate() {
        return new KafkaTemplate<>(producerFactory());
    }

}
