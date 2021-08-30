package com.example.kafka.kafkademo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class MessageController {

    private static final String TOPIC = "new-topic";
    private static final Logger logger= LoggerFactory.getLogger(MessageController.class);

    @Autowired
    KafkaTemplate<String,String> kafkaTemplate;

    @PostMapping("/message")
    public void sendMessage(@RequestParam String msg,@RequestParam String key){
        kafkaTemplate.send(TOPIC,key,msg);
        logger.info("Msg - {} sent to topic - {}",msg,TOPIC);

    }

    @KafkaListener(topics = {TOPIC})
    public void consumeMessage(String msg){

        logger.info("Msg - {} received on consumerFun1 ",msg);
    }
    @KafkaListener(topics = {TOPIC})
    public void consumeMessage2(String msg){
        logger.info("Msg- {} receivd  on consumerFun2",msg);
    }



}
