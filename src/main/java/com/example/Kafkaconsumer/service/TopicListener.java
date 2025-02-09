package com.example.Kafkaconsumer.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.KafkaListenerContainerFactory;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Service;

@Slf4j
@RequiredArgsConstructor
@Service
public class TopicListener {

    @Value("${topic.name.consumer")
    private String topicName;

    @Value("$spring.kafka.consumer.group-id")
    private String groupID;

    @KafkaListener(topics = "${topic.name.consumer}", groupId = "${spring.kafka.consumer.group-id}")
    public void consume(ConsumerRecord<String, String> payload, Acknowledgment acknowledgment) throws InterruptedException {

        String logMessage = String.format( "tópic: %1$s --- partition : %2$s --- message : %3$s --- offset : %4$s" ,
                payload.topic(), payload.partition(), payload.value(), payload.offset());
        Thread.sleep(400);

        if (payload.value().endsWith("5")) {
            log.info("commit failed for message " + payload.value());
            throw new RuntimeException("Simulated error while processing the message");
        } else {
            acknowledgment.acknowledge();
            log.info(logMessage + " committed");
        }


    }

}