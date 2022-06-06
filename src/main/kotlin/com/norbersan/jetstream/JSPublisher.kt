package com.norbersan.jetstream

import io.nats.client.JetStream
import io.nats.client.api.PublishAck
import org.slf4j.LoggerFactory
import java.util.concurrent.CompletableFuture

/**
 * Main class to publish messages through JetStream
 *
 * The publisher is totaly unaware of the way messages are
 * asociated) and the way they are consumed by the existing consumers-subscribers
 * so it needs no reference any of them neither reference any stream
 *
 * This class offers two different usages:
 *
 * 1- Creating a publisher dedicated publish to a fixed subject or set of subjects
 *
 * 2- Use static methods to publish messages
 *
 * Messages can be sent sync or asynchronously
 */
@Suppress("UNUSED")
class JSPublisher(private val js: JetStream,
                  private vararg val subjects: String
) {
    private val log = LoggerFactory.getLogger(javaClass)

    companion object{

        /**
         *  Publishes synchronously a JetStream message
         */
        fun publish(js: JetStream, subject: String, bytes: ByteArray): PublishAck? {
            return js.publish(subject,bytes)
        }

        /**
         *  Publishes asynchronously a JetStream message
         */
        fun publishAsync(js: JetStream, subject: String, bytes: ByteArray): CompletableFuture<PublishAck>? {
            return js.publishAsync(subject,bytes)
        }
    }

    /**
     * Publishes synchronously a JetStream message
     */
    fun publish(bytes: ByteArray): MutableList<PublishAck> {
        val result = mutableListOf<PublishAck>()
        subjects.forEach {
            log.info("JetStream sync-publication -> subject: ${it}; message: ${String(bytes)}")
            result.add(js.publish(it, bytes))
        }
        return result
    }

    /**
     * Publishes asynchronously a JetStream message
     */
    fun publishAsync(bytes: ByteArray): MutableList<CompletableFuture<PublishAck>> {
        val result = mutableListOf<CompletableFuture<PublishAck>>()
        subjects.forEach {
            log.info("JetStream async-publication -> subject: ${it}; message: ${String(bytes)}")
            result.add(js.publishAsync(it, bytes))
        }
        return result
    }
}