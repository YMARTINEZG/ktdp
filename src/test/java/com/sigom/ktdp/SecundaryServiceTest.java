package com.sigom.ktdp;

import com.sigom.ktdp.datagenerator.AbstractWordCountEventGenerator;
import com.sigom.ktdp.datagenerator.KafkaTemplateWordCountEventGenerator;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import org.junit.jupiter.api.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.annotation.DirtiesContext;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

@EmbeddedKafka(
        bootstrapServersProperty = "spring.kafka.bootstrap-servers",
        topics = {
                SecundaryServiceTest.INPUT_TOPIC, SecundaryServiceTest.OUTPUT_TOPIC
        })
@SpringBootTest(
        properties = {
                "spring.cloud.stream.kafka.streams.binder.configuration.commit.interval.ms=1000",
                "spring.cloud.stream.kafka.streams.binder.configuration.cache.max.bytes.buffering=0"
        })
@DirtiesContext
public class SecundaryServiceTest extends AbstractWordCountTest {

    static final String INPUT_TOPIC = "input-topic";
    static final String OUTPUT_TOPIC = "output-topic";
    static final String GROUP_NAME = "group-test";
    private static DefaultKafkaConsumerFactory<String, String> cf;

    @Autowired
    private EmbeddedKafkaBroker embeddedKafka;

    @BeforeEach
    void init() {

        Map<String, Object> props = new HashMap<>();

        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, embeddedKafka.getBrokersAsString());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        //DefaultKafkaProducerFactory<String, String> pf = new DefaultKafkaProducerFactory<>(props);

        AbstractWordCountEventGenerator eventGenerator = new
                KafkaTemplateWordCountEventGenerator(props, INPUT_TOPIC);
        setEventGenerator(eventGenerator);

        Map<String, Object> consumerProps = KafkaTestUtils.consumerProps(GROUP_NAME, "true", embeddedKafka);
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps.put(ConsumerConfig.CLIENT_ID_CONFIG, "test");
        consumerProps.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, 100);

//        consumerProps.put(JsonDeserializer.TRUSTED_PACKAGES, KafkaStreamsInventoryCountTests.class.getPackage().getName());
//        consumerProps.put(JsonDeserializer.KEY_DEFAULT_TYPE, ProductKey.class);
//        consumerProps.put(JsonDeserializer.VALUE_DEFAULT_TYPE, InventoryCountEvent.class);
//        consumerProps.put(JsonDeserializer.USE_TYPE_INFO_HEADERS, "false");

        cf = new DefaultKafkaConsumerFactory<>(consumerProps);
        consumer = cf.createConsumer(GROUP_NAME);
        consumer.subscribe(Collections.singleton(OUTPUT_TOPIC));
     }
}
