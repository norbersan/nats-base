package com.norbersan.jetstream

import io.nats.client.Connection
import io.nats.client.JetStream
import io.nats.client.MessageHandler
import io.nats.client.PushSubscribeOptions
import io.nats.client.api.AckPolicy
import io.nats.client.api.ConsumerConfiguration
import io.nats.client.api.DeliverPolicy
import org.slf4j.LoggerFactory
import java.time.Duration

class JetStreamQueuedSubscriber(val nc: Connection,
                                js: JetStream,
                                streamName: String,
                                subject: String,
                                queue: String,
                                handler: MessageHandler) {

    private val dispatcher = nc.createDispatcher()
    private val consumerConf: ConsumerConfiguration = ConsumerConfiguration.builder().apply {
        durable("${queue}@@@${subject.replace('.','@')}")
        ackPolicy(AckPolicy.Explicit)
        ackWait(Duration.ofSeconds(60))
        deliverPolicy(DeliverPolicy.All)
    }.build()
    val pushOpts = PushSubscribeOptions.builder()
        .configuration(consumerConf)
        .build()

    init {
        val subscription = js.subscribe(subject, queue, dispatcher, handler, false, pushOpts)
        //val subscription = js.subscribe(subject, queue, pushOpts)
        nc.flush(Duration.ofMillis(500))
//        Thread{
//            while(true){
//                LoggerFactory.getLogger(javaClass).info("asking server for messages")
//                handler.onMessage(subscription.nextMessage(Duration.ofSeconds(2)))
//            }
//        }.start()
    }
}