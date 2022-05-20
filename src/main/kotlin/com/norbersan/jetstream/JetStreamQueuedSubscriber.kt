package com.norbersan.jetstream

import io.nats.client.Connection
import io.nats.client.JetStream
import io.nats.client.MessageHandler

class JetStreamQueuedSubscriber(nc: Connection,
                                js: JetStream,
                                streamName: String,
                                subject: String,
                                queue: String,
                                handler: MessageHandler) {

    init {
    }
}