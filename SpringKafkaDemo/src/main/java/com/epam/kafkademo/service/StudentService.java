package com.epam.kafkademo.service;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.stereotype.Service;

import reactor.core.publisher.Flux;

@Service
public class StudentService {

	private final static String TOPIC = "student";
	static Flux<String> students2 = Flux.empty();

	private static Consumer<String, String> createConsumer() {
		Map<String, Object> props=new HashMap<>();
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		props.put(ConsumerConfig.GROUP_ID_CONFIG,
                "group1");
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
		
		// Create the consumer using props.
		final Consumer<String, String> consumer = new KafkaConsumer<>(props);

		// Subscribe to the topic.
		consumer.subscribe(Collections.singletonList(TOPIC));
		return consumer;
	}

	static void runConsumer() {
		System.out.println("hi amm");
		final Consumer<String, String> consumer = createConsumer();
		
		final int giveUp = 10;
		int noRecordsCount = 0;

		while (true) {
			
			final ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofMillis(100));
			if (consumerRecords.count() == 0) {
				noRecordsCount++;
				if (noRecordsCount > giveUp)
					break;
				else
					continue;
			}

			consumerRecords.forEach(record -> {
				System.out.println("hi amm");
				System.out.println("record "+record.value());
				Flux<String> data=Flux.just(String.format("Consumer Record:(%s, %s, %d, %d)\n", record.key(), record.value(),
						record.partition(), record.offset()));
				students2=Flux.concat(students2,data);
			});
			System.out.println("hi amm 2");
			consumer.commitAsync();
		}
		consumer.close();
	}

	Flux<String> students = Flux.empty();

	@KafkaListener(topics = "student", groupId = "group1")
	void listener(String student) {
		Flux<String> student1 = Flux.just(student);
		this.students = Flux.concat(this.students, student1);
	}

	public Flux<String> getStudents() {
		return this.students;
	}
	public Flux<String> getStudents2(){
		StudentService.runConsumer();
		return students2;
	}

}
