package com.norbersan.jetstream

import com.norbersan.common.deleteConsumerIfExists
import io.nats.client.*
import io.nats.client.api.AckPolicy
import io.nats.client.api.ConsumerConfiguration
import io.nats.client.api.DeliverPolicy
import java.time.Duration
import java.util.UUID
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit


// TODO durable consumer in this case needs to be released/removed/deleted
class JSPullSubscriber(private val nc: Connection,
                       js: JetStream,
                       subject: String,
                       handler: MessageHandler,
                       private val deleteConsumerOnShutdown: Boolean = true
) {

    private val latch = CountDownLatch(1)
    val prefix = if(deleteConsumerOnShutdown) UUID.randomUUID().toString() else "consumer"
    private val durableName = "${prefix}@@@${subject.replace('.','@')}"
    private val consumerConf: ConsumerConfiguration = ConsumerConfiguration.builder().apply {
        ackWait(60*1000)
        ackPolicy(AckPolicy.Explicit)
        deliverPolicy(DeliverPolicy.All)
        maxDeliver(4)
        backoff(Duration.ofSeconds(10),Duration.ofSeconds(20),Duration.ofSeconds(20))
        maxBatch(1)
    }.build()

    val pullOpts = PullSubscribeOptions.builder().apply {
        durable(durableName)
        configuration(consumerConf)
        }.build()

    private var started = true
    init {
        val subscription = js.subscribe(subject, pullOpts)
        nc.flush(Duration.ofMillis(500))
        Thread{
            while(started){
                subscription.pull(1)
                handler.onMessage(subscription.nextMessage(Duration.ofSeconds(1)))
            }
            latch.count
        }.start()
    }

    fun preDestroy(){
        started = false
        if (deleteConsumerOnShutdown){
            latch.await(10, TimeUnit.SECONDS)
            nc.deleteConsumerIfExists(durableName)
        }
    }
}