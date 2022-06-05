package com.norbersan.jetstream

import com.norbersan.common.NatsConnectionFactory
import com.norbersan.common.createStream
import com.norbersan.common.deleteStreamIfExists
import io.nats.client.Connection
import io.nats.client.MessageHandler
import io.nats.client.api.RetentionPolicy
import io.nats.client.api.StorageType
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import org.slf4j.LoggerFactory
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

class JSNonQueuedPushThrottledTest {
    val log = LoggerFactory.getLogger(javaClass)

    companion object{
        val factory = NatsConnectionFactory()
    }

    @Test
    fun `throttled messages`(){
        val conn: Connection = factory.getConnection("localhost", "","","","")
        val js = factory.jetStream(conn)
        val jsm = factory.jetStreamManagement(conn)
        val counter = AtomicInteger(0)

        Assertions.assertNotNull(jsm)

        jsm!!.deleteStreamIfExists("test")
        jsm!!.createStream("test", RetentionPolicy.Interest, StorageType.Memory, "subject.test")

        val publisher = JSPublisher(js, "subject.test")

        val handler =
            MessageHandler{
                if (it?.status?.isFlowControl == true){
                    log.info("flow control message ${String(it.data)}")
                }
                if (it.isStatusMessage){
                    log.info("received status message ( is flow control: ${it.status.isFlowControl}), ${String(it.data)}")
                }
                if (it.isJetStream){
                    log.info("received message #${counter.incrementAndGet()} subject: ${it.subject}, data: ${String(it.data)}")
                } else {
                    log.info("received message no jetstream")
                }
                it.ack()
            }

        val subscriber1 = JSPushSubscriber(conn, js, "test", "subject.test", handler)
        val subscriber2 = JSPushSubscriber(conn, js, "test", "subject.test", handler)
        val subscriber3 = JSPushSubscriber(conn, js, "test", "subject.test", handler)

        (1..100).forEach{ _ ->
            publisher.publish("*".repeat(600).encodeToByteArray()) // 600 * 8 = 4800 bits payload each message
        }

        TimeUnit.SECONDS.sleep(10)
        Assertions.assertEquals("Received 300 message(s)", "Received ${counter.get()} message(s)")
        jsm!!.deleteStreamIfExists("test")
    }
}