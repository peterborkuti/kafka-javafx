package com.example.kafka.kafkademo.services;

import org.apache.kafka.common.utils.Utils;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class KafkaProducerTest {
	@Test
	void murmur2() {
		int countZero = 0;
		for(int i = 0; i < 100; i++) {
			String key = "key" + i;
			int partition = Utils.toPositive(Utils.murmur2(key.getBytes())) % 2;
			if (partition == 0) countZero++;
			System.out.println(key + ":" + partition);
		}

		System.out.println(countZero);
	}

}