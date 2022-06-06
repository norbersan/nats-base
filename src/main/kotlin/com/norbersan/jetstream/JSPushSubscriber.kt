package com.norbersan.jetstream

import com.norbersan.common.deleteConsumerIfExists
import io.nats.client.Connection
import io.nats.client.JetStream
import io.nats.client.MessageHandler
import io.nats.client.PushSubscribeOptions
import io.nats.client.api.AckPolicy
import io.nats.client.api.ConsumerConfiguration
import java.time.Duration
import java.util.UUID

// TODO durable consumer in this case needs to be released/removed/deleted
// TODO remove stream name of constructor
class JSPushSubscriber(private val nc: Connection,
                       js: JetStream,
                       streamName: String,
                       subject: String,
                       handler: MessageHandler,
                       private val deleteConsumerOnShutdown: Boolean = true
) {
    constructor(nc: Connection,
                js: JetStream,
                streamName: String,
                subject: String,
                handler: MessageHandler) : this(nc, js, streamName, subject, handler,true)

    private val dispatcher = nc.createDispatcher()
    val prefix = if(deleteConsumerOnShutdown) UUID.randomUUID().toString() else "consumer"
    private val durableName = "${prefix}@@@${subject.replace('.','@')}"
    private val consumerConf: ConsumerConfiguration = ConsumerConfiguration.builder().apply {
        durable(durableName)
        ackPolicy(AckPolicy.Explicit)
        ackWait(Duration.ofSeconds(60))
        // TODO rateLimit used to throttle message delivery speed but not working as expected.
        // TO BE REMOVED because: 1) not working as expected 2) makes necessary streamName param in constructor
        if (!nc.jetStreamManagement().getConsumerNames(streamName).contains(durableName)){
            rateLimit(9000)
        }
    }.build()
    val pushOpts = PushSubscribeOptions.builder()
        .configuration(consumerConf)
        .build()

    init {
        js.subscribe(subject, dispatcher, handler, false, pushOpts)
    }

    fun preDestroy(){
        if (deleteConsumerOnShutdown) {
            nc.deleteConsumerIfExists(durableName)
        }
    }
}