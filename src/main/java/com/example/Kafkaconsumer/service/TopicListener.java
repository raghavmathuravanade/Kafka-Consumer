package com.example.Kafkaconsumer.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
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
    public void consume(ConsumerRecord<String, String> payload) throws InterruptedException {

        String logMessage = String.format( "tópic: %1$s --- partition : %2$s --- message : %3$s --- offset : %4$s" ,
                payload.topic(), payload.partition(), payload.value(), payload.offset());
        Thread.sleep(400);
        log.info(logMessage);

    }

}