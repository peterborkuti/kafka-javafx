/*
 * Copyright 2012-2018 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.example.kafka.kafkademo;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import javax.annotation.PreDestroy;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

@Component
@Slf4j
public class Producer {
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ScheduledExecutorService service;
    private ScheduledFuture scheduledFuture;
    private final Random rnd = new Random();
    private final byte[] bytes = new byte[1];


    @Value("${cloudkarafka.topic}")
    private String topic;

    private AtomicInteger delay = new AtomicInteger(500);

    Producer(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;

        service = Executors.newSingleThreadScheduledExecutor();
        service.schedule(() -> send(), delay.get(), TimeUnit.MILLISECONDS);
    }

    private void send() {
        rnd.nextBytes(bytes);
        this.kafkaTemplate.send(topic, "" + bytes[0]);
        log.debug("Sent sample message [" + bytes[0] + "] to " + topic);
        service.schedule(() -> send(), delay.get(), TimeUnit.MILLISECONDS);
    }

    public void setDelay(int delay) {
        this.delay.set(delay);
    }

    public int getDelay() {
        return delay.get();
    }

    @PreDestroy
    private void shutdown() throws InterruptedException {
        service.shutdown();
    }

}
