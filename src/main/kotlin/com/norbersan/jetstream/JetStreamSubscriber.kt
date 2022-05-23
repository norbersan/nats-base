package com.norbersan.jetstream

import io.nats.client.Connection
import io.nats.client.JetStream
import io.nats.client.MessageHandler
import io.nats.client.PushSubscribeOptions
import io.nats.client.api.AckPolicy
import io.nats.client.api.ConsumerConfiguration
import java.time.Duration

class JetStreamSubscriber(nc: Connection,
                          js: JetStream,
                          streamName: String,
                          subject: String,
                          handler: MessageHandler) {
    private val dispatcher = nc.createDispatcher()
    private val consumerConf: ConsumerConfiguration = ConsumerConfiguration.builder().apply {
        durable("${subject.replace('.','@')}")
        ackPolicy(AckPolicy.Explicit)
        ackWait(Duration.ofSeconds(60))
    }.build()
    val pushOpts = PushSubscribeOptions.builder()
        .configuration(consumerConf)
        .build()

    init {
        js.subscribe(subject, dispatcher, handler, false, pushOpts)
    }
}