package com.norbersan.jetstream

import io.nats.client.Connection
import io.nats.client.JetStream

class JetStreamQueuedPublisher(nc: Connection, private val js: JetStream, streamName: String, subject: String, queue: String) {
    fun publish(data: ByteArray){

    }
}