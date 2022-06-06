package com.norbersan.jetstream

import io.nats.client.Connection
import io.nats.client.JetStream
import io.nats.client.MessageHandler
import io.nats.client.PullSubscribeOptions
import io.nats.client.api.AckPolicy
import io.nats.client.api.ConsumerConfiguration
import io.nats.client.api.DeliverPolicy
import java.time.Duration

class JSQueuedPullSubscriber(nc: Connection,
                             js: JetStream,
                             subject: String,
                             queue: String,
                             handler: MessageHandler) {

    private val durableName = "pull-${queue}@@@${subject.replace('.','@')}"
    private val consumerConf: ConsumerConfiguration = ConsumerConfiguration.builder().apply {
        //ackWait(60*1000) // fails for no reason
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

    fun predestroy(){
        started = false
    }
}