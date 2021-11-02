package com.sigom.ktdp.datagenerator;

import lombok.extern.log4j.Log4j2;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;

import java.util.Map;

@Log4j2
public class KafkaTemplateWordCountEventGenerator extends AbstractWordCountEventGenerator {

    private final KafkaTemplate<String, String> kafkaTemplate;

    public KafkaTemplateWordCountEventGenerator(Map<String, Object> producerProperties, String destination) {
        DefaultKafkaProducerFactory<String, String> pf = new DefaultKafkaProducerFactory(producerProperties);
        kafkaTemplate = new KafkaTemplate<>(pf, true);
        kafkaTemplate.setDefaultTopic(destination);
    }
    @Override
    protected void doSendEvent(String key, String value) {
        kafkaTemplate.sendDefault(key, value);
    }
}
