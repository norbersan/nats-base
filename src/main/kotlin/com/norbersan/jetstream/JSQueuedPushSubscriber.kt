package com.norbersan.jetstream

import io.nats.client.Connection
import io.nats.client.JetStream
import io.nats.client.MessageHandler
import io.nats.client.PushSubscribeOptions
import io.nats.client.api.AckPolicy
import io.nats.client.api.ConsumerConfiguration
import io.nats.client.api.DeliverPolicy
import java.time.Duration

class JSQueuedPushSubscriber(nc: Connection,
                             js: JetStream,
                             subject: String,
                             queue: String,
                             handler: MessageHandler) {

    private val dispatcher = nc.createDispatcher()
    private val durableName = "push-${queue}@@@${subject.replace('.','@')}"
    private val consumerConf: ConsumerConfiguration = ConsumerConfiguration.builder().apply {
        durable(durableName)
        ackPolicy(AckPolicy.Explicit)
        deliverGroup(queue)
        ackWait(Duration.ofSeconds(60))
        deliverPolicy(DeliverPolicy.All)
    }.build()
    val pushOpts = PushSubscribeOptions.builder()
        .configuration(consumerConf)
        .build()

    init {
        js.subscribe(subject, queue, dispatcher, handler, false, pushOpts)
        nc.flush(Duration.ofMillis(500))
    }
}