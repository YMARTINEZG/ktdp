package com.sigom.ktdp;

import com.sigom.ktdp.datagenerator.AbstractWordCountEventGenerator;
import lombok.extern.log4j.Log4j2;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.test.utils.KafkaTestUtils;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

@Log4j2
public abstract class AbstractWordCountTest {

    private static AbstractWordCountEventGenerator eventGenerator;
    protected Consumer<String, String> consumer;

    protected static void setEventGenerator(AbstractWordCountEventGenerator eventGenerator) {
        AbstractWordCountTest.eventGenerator = eventGenerator;
    }

    @Test
    public void secondaryProcessorApplicationTest() {

        Map<String, String> expectedCounts = eventGenerator.generateRandomEvents(1, 3);

        Map<String, String> actualEvents = consumeActualWordCountEvents(3);

        assertThat(actualEvents).hasSize(1);

        expectedCounts.forEach((key, value) ->
                assertThat(actualEvents.get(key)).isEqualTo(value.toUpperCase()));

//        assertThat(singleRecord).isNotNull();
//        assertThat(singleRecord.key()).isEqualTo("this is a test world");
//        assertThat(singleRecord.value()).isEqualTo("THIS IS A TEST WORLD");
    }

    @AfterEach
    void tearDown() {

        Map<String, String> events = eventGenerator.reset();
        consumeActualWordCountEvents(events.size());

        if (consumer != null) {
            consumer.close();
        }
    }
    /**
     * Consume the actual events from the output topic.
     * This implementation uses a {@link Consumer}, assuming a (an embedded) Kafka Broker but may be overridden.
     * @param expectedCount the expected number of messages is known. This avoids a timeout delay if all is well.
     *
     * @return the consumed data.
     */
    protected Map<String, String> consumeActualWordCountEvents(int expectedCount) {
        Map<String, String> inventoryCountEvents = new LinkedHashMap<>();
        int receivedCount = 0;
        while (receivedCount < expectedCount) {
            ConsumerRecords<String, String> records = KafkaTestUtils.getRecords(consumer, 1000);
            if (records.isEmpty()) {
                log.error("No more records received. Expected {} received {}.", expectedCount, receivedCount);
                break;
            }
            receivedCount += records.count();
            for (Iterator<ConsumerRecord<String, String>> it = records.iterator(); it.hasNext(); ) {
                ConsumerRecord<String, String> consumerRecord = it.next();
                log.debug("consumed " + consumerRecord.key() + " = " + consumerRecord.value());
                inventoryCountEvents.put(consumerRecord.key(), consumerRecord.value());
            }
        }
        return inventoryCountEvents;
    }
}
