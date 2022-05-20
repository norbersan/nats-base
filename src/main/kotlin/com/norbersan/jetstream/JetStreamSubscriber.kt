package com.norbersan.jetstream

import io.nats.client.Connection
import io.nats.client.JetStream
import io.nats.client.MessageHandler
import io.nats.client.PushSubscribeOptions
import io.nats.client.api.AckPolicy
import io.nats.client.api.ConsumerConfiguration
import java.time.Duration

class JetStreamSubscriber(nc: Connection, js: JetStream, streamName: String, subjects: Map<String, MessageHandler>) {
    init {
        subjects.entries.forEach{
            js.subscribe(it.key, nc.createDispatcher(), it.value, false,
                PushSubscribeOptions.builder().apply {
                    configuration(ConsumerConfiguration.builder()
                        .durable(it.key.replace('.', '@'))
                        .ackPolicy(AckPolicy.Explicit)
                        .ackWait(Duration.ofSeconds(60))
                        .build())
                }.build()
            )
        }
    }
}