package com.norbersan.jetstream

import com.norbersan.common.NatsConnectionFactory
import com.norbersan.common.createStream
import com.norbersan.common.deleteStreamIfExists
import com.norbersan.common.logStreamsAndConsumers
import io.nats.client.Connection
import io.nats.client.MessageHandler
import io.nats.client.api.RetentionPolicy
import io.nats.client.api.StorageType
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import org.slf4j.LoggerFactory
import java.time.Duration
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

class JSQueuedPushTest {
    private val log = LoggerFactory.getLogger(javaClass)

    companion object{
        val factory = NatsConnectionFactory()
    }

    @Test
    fun `single message published synchronously and received by one single subscriber out of three`(){
        val conn: Connection = factory.getConnection(host = "localhost")
        val js = factory.jetStream(conn)
        val jsm = factory.jetStreamManagement(conn)
        val counter = AtomicInteger(0)

        jsm!!.deleteStreamIfExists("test")

        jsm.logStreamsAndConsumers("At the beginning of the test")
        Assertions.assertNotNull(jsm)

        jsm.createStream("test", RetentionPolicy.WorkQueue, StorageType.Memory, "subject.test")

        jsm.logStreamsAndConsumers("After creating stream, before any object subscribed")
        val publisher = JSPublisher(js, "subject.test")
        val handler = MessageHandler{
            if (it == null){
                return@MessageHandler
            } else if (it.isJetStream){
                counter.incrementAndGet()
                log.info("received message subject: ${it.subject}, data: ${String(it.data)}")
            } else {
                log.info("received message no jetstream")
            }
        }

        val subscriber1 = JSQueuedPushSubscriber(conn, js, "subject.test", "queue", handler)
        val subscriber2 = JSQueuedPushSubscriber(conn, js, "subject.test", "queue", handler)
        val subscriber3 = JSQueuedPushSubscriber(conn, js, "subject.test", "queue", handler)

        jsm.logStreamsAndConsumers("After objects subscribed, before any publication")

        publisher.publish("test message".encodeToByteArray())
        conn.flush(Duration.ofMillis(500))

        TimeUnit.SECONDS.sleep(10)
        jsm.logStreamsAndConsumers("After first message published")
        Assertions.assertEquals("Received 1 message(s)", "Received ${counter.get()} message(s)")

        jsm.logStreamsAndConsumers("At the end of the test")
    }
}