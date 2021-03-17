package com.example.kafka.kafkademo.config;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.config.SecurityConfig;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.kafka.KafkaAutoConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

@Configuration
public class KafkaConfig {
	@Value("${cloudkarafka.topic}")
	private String topic;

	@Value("${spring.kafka.bootstrap-servers}")
	private String servers;

	@Value("${spring.kafka.properties.security.protocol}")
	private String protocol;

	@Value("${spring.kafka.properties.sasl.mechanism}")
	private String mechanism;

	@Value("${spring.kafka.properties.sasl.jaas.config}")
	private String jaasConfig;

	@Value("${spring.kafka.consumer.group-id}")
	private String groupId;

	@Value("${spring.kafka.consumer.auto-offset-reset}")
	private String autoOffsetReset;

	@Value("${spring.kafka.consumer.value-deserializer}")
	private String deserializer;

	@Value("${spring.kafka.producer.value-serializer}")
	private String serializer;

	@Value("${spring.kafka.consumer.properties.spring.json.trusted.packages}")
	private String trustedPackages;


	@Bean
	public KafkaReceiver<String, String> kafkaReceiver() {
		Map<String, Object> props = new HashMap<>();
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, servers);
		props.put(ConsumerConfig.CLIENT_ID_CONFIG, "consumer-ui-" + getRandomId());
		props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class);
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, autoOffsetReset);
		props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
		props.put("sasl.mechanism", mechanism);
		props.put("security.protocol",protocol);
		props.put("sasl.jaas.config", jaasConfig);

		final ReceiverOptions<String, String> receiverOptions =
				ReceiverOptions.<String, String>create(props)
						.subscription(Collections.singleton(topic));
		return KafkaReceiver.create(receiverOptions);
	}

	public static String getRandomId() {
		return String.format("%08d", getRandomIntId());
	}

	public static int getRandomIntId() {
		return Math.abs(new Random().nextInt());
	}
}
