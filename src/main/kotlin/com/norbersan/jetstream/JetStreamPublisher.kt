package com.norbersan.jetstream

import io.nats.client.JetStream
import org.slf4j.LoggerFactory

class JetStreamPublisher(val js: JetStream, vararg val subjects: String) {
    val log = LoggerFactory.getLogger(javaClass)

    fun publish(bytes: ByteArray){
        subjects.forEach {
            js.publish(it, bytes)
            log.info("JetStream sync-pub subject: ${it}; message: ${String(bytes)}")
        }
    }

    fun publishAsync(bytes: ByteArray){
        subjects.forEach {
            js.publishAsync(it, bytes)
            log.info("JetStream async-pub subject: ${it}; message: ${String(bytes)}")
        }
    }
}