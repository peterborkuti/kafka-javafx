package com.example.kafka.kafkademo.controller;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;
import com.example.kafka.kafkademo.Consumer;

@RequiredArgsConstructor
@RestController
@Slf4j
public class KafkaController {
	private final Consumer consumer;

	@RequestMapping(value = "/kafka", method = RequestMethod.GET, produces = MediaType.TEXT_EVENT_STREAM_VALUE)
	public Flux<String> getNumbers() {
		return consumer.getReceiver().map(r -> {
			log.debug("got:" + r.value());
			return r.value();
		});
	}
}
