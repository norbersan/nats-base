package com.norbersan.jetstream

import io.nats.client.Connection
import io.nats.client.JetStream
import io.nats.client.MessageHandler
import io.nats.client.PushSubscribeOptions
import io.nats.client.api.AckPolicy
import io.nats.client.api.ConsumerConfiguration
import java.time.Duration
import java.util.UUID

class JetStreamSubscriber(nc: Connection,
                          js: JetStream,
                          streamName: String,
                          subject: String,
                          handler: MessageHandler) {
    private val dispatcher = nc.createDispatcher()
//    private val durableName = "${subject.replace('.','@')}"
    private val durableName = UUID.randomUUID().toString()
    private val consumerConf: ConsumerConfiguration = ConsumerConfiguration.builder().apply {
        durable(durableName)
        ackPolicy(AckPolicy.Explicit)
        ackWait(Duration.ofSeconds(60))
        if (!nc.jetStreamManagement().getConsumerNames(streamName).contains(durableName)){
            rateLimit(32)
            flowControl(250)
        }
    }.build()
    val pushOpts = PushSubscribeOptions.builder()
        .configuration(consumerConf)
        .build()

    init {
        js.subscribe(subject, dispatcher, handler, false, pushOpts)
    }
}