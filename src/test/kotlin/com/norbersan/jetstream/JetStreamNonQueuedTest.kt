package com.norbersan.jetstream

import com.norbersan.common.NatsConnectionFactory
import com.norbersan.common.createStream
import io.nats.client.Connection
import io.nats.client.MessageHandler
import io.nats.client.api.StorageType
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import org.slf4j.LoggerFactory
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

class JetStreamNonQueuedTest {
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

        jsm!!.createStream("test", StorageType.Memory, "subject.test")

        val publisher = JetStreamPublisher(js, "subject.test")
        val subscriber = JetStreamSubscriber(conn, js, "test", mapOf("subject.test" to
                MessageHandler{
                    if (it.isJetStream){
                        Assertions.assertTrue(counter.decrementAndGet() == 0)
                        log.info("received message subject: ${it.subject}, data: ${String(it.data)}")
                    } else{
                        log.info("received message no jetstream")
                    }
                }))

        counter.incrementAndGet()
        publisher.publish("test message".encodeToByteArray())

        TimeUnit.SECONDS.sleep(1)
        Assertions.assertTrue(counter.get() == 0)
        jsm.deleteStream("test")
    }

    @Test
    fun `single message published asynchronously and received by single subscriber`(){
        val conn: Connection = factory.getConnection("localhost", "","","","")
        val js = factory.jetStream(conn)
        val jsm = factory.jetStreamManagement(conn)
        val counter = AtomicInteger(0)

        Assertions.assertNotNull(jsm)

        jsm!!.createStream("test", StorageType.Memory, "subject.test")

        val publisher = JetStreamPublisher(js, "subject.test")
        val subscriber = JetStreamSubscriber(conn, js, "test", mapOf("subject.test" to
                MessageHandler{
                    if (it.isJetStream){
                        Assertions.assertTrue(counter.decrementAndGet() == 0)
                        log.info("received message subject: ${it.subject}, data: ${String(it.data)}")
                    } else{
                        log.info("received message no jetstream")
                    }
                }))

        counter.incrementAndGet()
        publisher.publishAsync("test message".encodeToByteArray())

        TimeUnit.SECONDS.sleep(1)
        Assertions.assertTrue(counter.get() == 0)
        jsm.deleteStream("test")
    }

    @Test
    fun `single message published synchronously and received by three subscribers`(){
        val conn: Connection = factory.getConnection("localhost", "","","","")
        val js = factory.jetStream(conn)
        val jsm = factory.jetStreamManagement(conn)
        val counter = AtomicInteger(0)

        Assertions.assertNotNull(jsm)

        jsm!!.createStream("test", StorageType.Memory, "subject.test")

        val publisher = JetStreamPublisher(js, "subject.test")

        val map = mapOf("subject.test" to
                MessageHandler{
                    if (it.isJetStream){
                        log.info("received message #${counter.incrementAndGet()} subject: ${it.subject}, data: ${String(it.data)}")
                    } else{
                        log.info("received message no jetstream")
                    }
                })

        val subscriber1 = JetStreamSubscriber(conn, js, "test", map)
        val subscriber2 = JetStreamSubscriber(conn, js, "test", map)
        val subscriber3 = JetStreamSubscriber(conn, js, "test", map)

        publisher.publish("test message".encodeToByteArray())

        TimeUnit.SECONDS.sleep(1)
        Assertions.assertTrue(counter.get() == 3)
        jsm.deleteStream("test")
    }

    @Test
    fun `single message published asynchronously and received by three subscribers`(){
        val conn: Connection = factory.getConnection("localhost", "","","","")
        val js = factory.jetStream(conn)
        val jsm = factory.jetStreamManagement(conn)
        val counter = AtomicInteger(0)

        Assertions.assertNotNull(jsm)

        jsm!!.createStream("test", StorageType.Memory, "subject.test")

        val publisher = JetStreamPublisher(js, "subject.test")

        val map = mapOf("subject.test" to
                MessageHandler{
                    if (it.isJetStream){
                        log.info("received message #${counter.incrementAndGet()} subject: ${it.subject}, data: ${String(it.data)}")
                    } else{
                        log.info("received message no jetstream")
                    }
                })

        val subscriber1 = JetStreamSubscriber(conn, js, "test", map)
        val subscriber2 = JetStreamSubscriber(conn, js, "test", map)
        val subscriber3 = JetStreamSubscriber(conn, js, "test", map)

        publisher.publishAsync("test message".encodeToByteArray())

        TimeUnit.SECONDS.sleep(1)
        Assertions.assertTrue(counter.get() == 3)
        jsm.deleteStream("test")
    }
}