package com.sigom.ktdp.binding;

import org.apache.kafka.streams.kstream.KStream;
import org.springframework.cloud.stream.annotation.Input;
import org.springframework.cloud.stream.annotation.Output;

public interface ListenerBinding {
    @Input("process-in-0")
    KStream<String,String> transactionStream();
    @Output("process-out-0")
    KStream<String,String> customerStream();
}
