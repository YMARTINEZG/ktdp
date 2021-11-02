package com.sigom.ktdp.datagenerator;

import lombok.extern.log4j.Log4j2;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

@Log4j2
public abstract class AbstractWordCountEventGenerator {

    private final Map<String, String> accumulatedWordCounts = new LinkedHashMap<>();

    public Map<String, String> generateRandomEvents(int numberKeys, int eventsPerKey) {
        return doGenerateEvents(numberKeys, eventsPerKey);
    }
    private Map<String, String> doGenerateEvents(int numberKeys, int eventsPerKey) {
        if(eventsPerKey == 3){
            accumulatedWordCounts.put(String.valueOf(numberKeys), "hello world");
            accumulatedWordCounts.put(String.valueOf(numberKeys), "spring boot");
            accumulatedWordCounts.put(String.valueOf(numberKeys), "hello spring");
        } else {
            accumulatedWordCounts.put(String.valueOf(numberKeys), "hello world");
        }
        return Collections.unmodifiableMap(new LinkedHashMap<>(accumulatedWordCounts));
    }
    public Map<String, String> reset() {
        Map<String, String> current
                = Collections.unmodifiableMap(new LinkedHashMap(accumulatedWordCounts));

        accumulatedWordCounts.keySet().forEach(key -> {
            sendEvent(key, "hello");
        });
        accumulatedWordCounts.clear();
        return current;
    }
    protected void sendEvent(String key, String value) {
        log.debug("Sending wordCountEvent: key {} value {}",
                key, value);
        doSendEvent(key, value);
    }

    protected abstract void doSendEvent(String key, String value);
}
