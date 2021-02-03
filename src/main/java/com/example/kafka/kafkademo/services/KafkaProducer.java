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

package com.example.kafka.kafkademo.services;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@Service
@Slf4j
@RequiredArgsConstructor
public class KafkaProducer {
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ScheduledExecutorService service = Executors.newSingleThreadScheduledExecutor();

    @Value("${cloudkarafka.topic}")
    private String topic;

    @Getter
    private int sendingNumber = (int)Math.floor(Math.random()*256 - 128);

    @Setter
    @Getter
    private Integer delay = 500;

    public void setSendingNumber(int number) {
        sendingNumber = Math.max(-128, Math.min(127, number));
    }

    @PostConstruct
    private void init() {
        service.schedule(() -> send(), delay, TimeUnit.MILLISECONDS);
    }

    private void send() {
        this.kafkaTemplate.send(topic, "" + sendingNumber);
        log.debug("Sent sample message [" + sendingNumber + "] to " + topic);
        service.schedule(() -> send(), delay, TimeUnit.MILLISECONDS);
    }

    @PreDestroy
    private void shutdown() throws InterruptedException {
        service.shutdown();
    }

}
