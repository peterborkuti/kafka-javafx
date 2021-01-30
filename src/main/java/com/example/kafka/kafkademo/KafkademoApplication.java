package com.example.kafka.kafkademo;

import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;

@SpringBootApplication
public class KafkademoApplication {
	private static ApplicationContext applicationContext;

	public static void main(String[] args) {
		applicationContext = SpringApplication.run(KafkademoApplication.class, args);
		displayAllBeans();
	}

	public static void displayAllBeans() {
		String[] allBeanNames = applicationContext.getBeanDefinitionNames();
		for(String beanName : allBeanNames) {
			System.out.println(beanName);
		}
	}


	@Bean
	public ApplicationRunner runner(Producer producer) {
		return (args) -> {
			for(int i = 1; i < 20; i++) {
				producer.send(new SampleMessage(i, "A simple test message"));
			}
		};
	}
}
