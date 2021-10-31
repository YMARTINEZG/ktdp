package com.sigom.ktdp.services;

import com.sigom.ktdp.binding.ListenerBinding;
import lombok.extern.log4j.Log4j2;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KStream;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.annotation.StreamListener;
import org.springframework.messaging.handler.annotation.SendTo;
import org.springframework.stereotype.Service;

@Service
@Log4j2
@EnableBinding(ListenerBinding.class)
public class MainListenerService {

    @StreamListener("process-in-0")
    @SendTo("process-out-0")
    public KStream<String, String> process(KStream<String, String> input){
        input.foreach((k,v) -> log.info("key = "+ k + " values = " + v));
        return input.map((k,v) -> KeyValue.pair(k, v.toUpperCase()));
    }
}
