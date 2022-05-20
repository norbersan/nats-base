package com.norbersan.jetstream

import com.norbersan.common.NatsConnectionFactory
import com.norbersan.common.createStream
import io.nats.client.Connection
import io.nats.client.MessageHandler
import io.nats.client.api.StorageType
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import org.slf4j.LoggerFactory

class JetStreamTest {
    val log = LoggerFactory.getLogger(javaClass)

    companion object{
        val factory = NatsConnectionFactory()
    }

    @Test
    fun test(){
        val conn: Connection = factory.getConnection("localhost", "","","","")
        val js = factory.jetStream(conn)
        val jsm = factory.jetStreamManagement(conn)

        Assertions.assertNotNull(jsm)

        jsm!!.createStream("test", StorageType.Memory, "subject.test")

        val publisher = JetStreamPublisher(js, "subject.test")
        val subscriber= JetStreamSubscriber(conn, js, "test", mapOf("subject.test" to
            MessageHandler{
                if (it.isJetStream){
                    log.info("received message subject: ${it.subject}, data: ${String(it.data)}")
                } else{
                    log.info("received message no jetstream")
                }
            }))

        publisher.publish("test message".encodeToByteArray())

        jsm.deleteStream("test")
    }
}