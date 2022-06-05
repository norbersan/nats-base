package com.norbersan.jetstream

import io.nats.client.*
import io.nats.client.api.AckPolicy
import io.nats.client.api.ConsumerConfiguration
import io.nats.client.api.DeliverPolicy
import java.time.Duration
import java.util.UUID

class JSPullSubscriber(nc: Connection,
                       js: JetStream,
                       streamName: String,
                       subject: String,
                       handler: MessageHandler) {

    private val durableName = "${UUID.randomUUID()}@@@${subject.replace('.','@')}"
    private val consumerConf: ConsumerConfiguration = ConsumerConfiguration.builder().apply {
        ackWait(60*1000)
        ackPolicy(AckPolicy.Explicit)
        deliverPolicy(DeliverPolicy.All)
        maxDeliver(4)
        backoff(Duration.ofSeconds(10),Duration.ofSeconds(20),Duration.ofSeconds(20))
        maxBatch(1)
    }.build()

    val pullOpts = PullSubscribeOptions.builder()
        .durable(durableName)
        .configuration(consumerConf)
        .build()

    private var started = true
    init {
        val subscription = js.subscribe(subject, pullOpts)
        nc.flush(Duration.ofMillis(500))
        Thread{
            while(started){
                subscription.pull(1)
                handler.onMessage(subscription.nextMessage(Duration.ofSeconds(1)))
            }
        }.start()
    }

    fun stop(){
        started = false
    }
}