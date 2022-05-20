package com.norbersan.jetstream

import io.nats.client.JetStream
import org.slf4j.LoggerFactory

class JetStreamPublisher(private val js: JetStream, private vararg val subjects: String) {
    val log = LoggerFactory.getLogger(javaClass)

    fun publish(bytes: ByteArray){
        subjects.forEach {
            log.info("JetStream sync-publication -> subject: ${it}; message: ${String(bytes)}")
            js.publish(it, bytes)
        }
    }

    fun publishAsync(bytes: ByteArray){
        subjects.forEach {
            log.info("JetStream async-publication -> subject: ${it}; message: ${String(bytes)}")
            js.publishAsync(it, bytes)
        }
    }
}