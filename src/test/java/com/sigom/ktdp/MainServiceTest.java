package com.sigom.ktdp;

import lombok.extern.log4j.Log4j2;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.rule.EmbeddedKafkaRule;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.kafka.test.utils.KafkaTestUtils.consumerProps;
import static org.springframework.kafka.test.utils.KafkaTestUtils.getRecords;

@Log4j2
@RunWith(SpringRunner.class)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
@SpringBootTest(
        webEnvironment = SpringBootTest.WebEnvironment.NONE,
        properties = {"server.port=0"}
)
public class MainServiceTest {
    static final String INPUT_TOPIC = "input-topic";
    static final String OUTPUT_TOPIC = "output-topic";
    private static final String GROUP_NAME = "group-test";

    @ClassRule
    public static EmbeddedKafkaRule embeddedKafkaRule = new EmbeddedKafkaRule(1, false, 1,
            INPUT_TOPIC, OUTPUT_TOPIC);

    private static final EmbeddedKafkaBroker embeddedKafka = embeddedKafkaRule.getEmbeddedKafka();

    public static KafkaTemplate<String, String> template;
    public static Consumer<String, String> consumer;

    @BeforeClass
    public static void setUp() {

        System.setProperty("spring.kafka.bootstrap-servers", embeddedKafka.getBrokersAsString());

        Map<String, Object> senderProps = KafkaTestUtils.producerProps(embeddedKafka);
        senderProps.put("key.serializer", StringSerializer.class);
        senderProps.put("value.serializer", StringSerializer.class);
        DefaultKafkaProducerFactory<String, String> pf = new DefaultKafkaProducerFactory<>(senderProps);
        template = new KafkaTemplate<>(pf, true);


        Map<String, Object> consumerProps = consumerProps(GROUP_NAME, "false", embeddedKafka);
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        DefaultKafkaConsumerFactory<String, String> cf = new DefaultKafkaConsumerFactory<>(consumerProps);
        consumer = cf.createConsumer();
        embeddedKafka.consumeFromAnEmbeddedTopic(consumer, OUTPUT_TOPIC);

    }
    @AfterClass
    public static void tearDown(){
        consumer.close();
    }

    @Test
    public void MainProcessorApplicationTest() {
        Set<String> actualResultSet = new HashSet<>();
        Set<String> expectedResultSet = new HashSet<>();
        expectedResultSet.add("HELLO1");
        expectedResultSet.add("HELLO2");
        expectedResultSet.add("MY TEST");
        expectedResultSet.add("TEST ANOTHER WORLD");

        template.send(INPUT_TOPIC, "one,","hello1");
        template.send(INPUT_TOPIC, "two","hello2");
        template.send(INPUT_TOPIC, "one","my test");
        template.send(INPUT_TOPIC, "six","test Another World");

        int receivedAll = 0;
        while(receivedAll<4) {
                ConsumerRecords<String, String> cr = getRecords(consumer);
                receivedAll = receivedAll + cr.count();
                cr.iterator().forEachRemaining(r -> actualResultSet.add(r.value()));
        }

        assertThat(actualResultSet.equals(expectedResultSet)).isTrue();
    }
    @Test
    public void singleEventProducerTest() {
        template.send(INPUT_TOPIC, "key1", "this is a test world");

        ConsumerRecord<String, String> singleRecord = KafkaTestUtils.getSingleRecord(consumer, OUTPUT_TOPIC);

        assertThat(singleRecord).isNotNull();
        assertThat(singleRecord.key()).isEqualTo("key1");
        assertThat(singleRecord.value()).isEqualTo("THIS IS A TEST WORLD");

    }

}
