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

class JSNonQueuedPushTest {
    val log = LoggerFactory.getLogger(javaClass)

    companion object{
        val factory = NatsConnectionFactory()
    }

    @Test
    fun `single message published synchronously and received by single subscriber`(){
        val conn: Connection = factory.getConnection("localhost", "","","","")
        val js = factory.jetStream(conn)
        val jsm = factory.jetStreamManagement(conn)
        val counter = AtomicInteger(0)

        Assertions.assertNotNull(jsm)

        jsm!!.deleteStreamIfExists("test")
        jsm!!.createStream("test", RetentionPolicy.Interest, StorageType.Memory, "subject.test")

        val publisher = JSPublisher(js, "subject.test")
        val subscriber = JSPushSubscriber(conn, js, "test", "subject.test") {
                    if (it.isJetStream){
                        Assertions.assertTrue(counter.decrementAndGet() == 0)
                        log.info("received message subject: ${it.subject}, data: ${String(it.data)}")
                    } else{
                        log.info("received message no jetstream")
                    }
                }

        counter.incrementAndGet()
        publisher.publish("test message".encodeToByteArray())

        TimeUnit.SECONDS.sleep(1)
        Assertions.assertTrue(counter.get() == 0)
        jsm!!.deleteStreamIfExists("test")
    }

    @Test
    fun `single message published asynchronously and received by single subscriber`(){
        val conn: Connection = factory.getConnection("localhost", "","","","")
        val js = factory.jetStream(conn)
        val jsm = factory.jetStreamManagement(conn)
        val counter = AtomicInteger(0)

        Assertions.assertNotNull(jsm)

        jsm!!.deleteStreamIfExists("test")
        jsm!!.createStream("test", RetentionPolicy.Interest, StorageType.Memory, "subject.test")

        val publisher = JSPublisher(js, "subject.test")
        val subscriber = JSPushSubscriber(conn, js, "test", "subject.test") {
            if (it.isJetStream) {
                Assertions.assertTrue(counter.decrementAndGet() == 0)
                log.info("received message subject: ${it.subject}, data: ${String(it.data)}")
            } else {
                log.info("received message no jetstream")
            }
        }

        counter.incrementAndGet()
        publisher.publishAsync("test message".encodeToByteArray())

        TimeUnit.SECONDS.sleep(1)
        Assertions.assertTrue(counter.get() == 0)
        jsm!!.deleteStreamIfExists("test")
    }

    @Test
    fun `single message published synchronously and received by three subscribers`(){
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
                    if (it.isJetStream){
                        log.info("received message #${counter.incrementAndGet()} subject: ${it.subject}, data: ${String(it.data)}")
                    } else{
                        log.info("received message no jetstream")
                    }
                }

        val subscriber1 = JSPushSubscriber(conn, js, "test", "subject.test", handler)
        val subscriber2 = JSPushSubscriber(conn, js, "test", "subject.test", handler)
        val subscriber3 = JSPushSubscriber(conn, js, "test", "subject.test", handler)

        publisher.publish("test message".encodeToByteArray())

        TimeUnit.SECONDS.sleep(1)
        Assertions.assertTrue(counter.get() == 3)
        jsm!!.deleteStreamIfExists("test")
    }

    @Test
    fun `single message published asynchronously and received by three subscribers`(){
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
                if (it.isJetStream){
                    log.info("received message #${counter.incrementAndGet()} subject: ${it.subject}, data: ${String(it.data)}")
                } else{
                    log.info("received message no jetstream")
                }
            }

        val subscriber1 = JSPushSubscriber(conn, js, "test", "subject.test", handler)
        val subscriber2 = JSPushSubscriber(conn, js, "test", "subject.test", handler)
        val subscriber3 = JSPushSubscriber(conn, js, "test", "subject.test", handler)

        publisher.publishAsync("test message".encodeToByteArray())

        TimeUnit.SECONDS.sleep(1)
        Assertions.assertTrue(counter.get() == 3)
        jsm!!.deleteStreamIfExists("test")
    }

}