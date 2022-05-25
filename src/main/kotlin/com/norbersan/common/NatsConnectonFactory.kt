package com.norbersan.common

import io.nats.client.*
import io.nats.client.api.RetentionPolicy
import io.nats.client.api.StorageType
import io.nats.client.api.StreamConfiguration
import io.nats.client.api.StreamInfo
import org.slf4j.LoggerFactory
import java.io.IOException
import java.time.Duration

class NatsConnectionFactory {
    val log = LoggerFactory.getLogger(javaClass)

    fun getConnection(
        host: String,
        port: String,
        server: String,
        jwt: String,
        seed: String,
        name: String = "default"
    ): Connection {
        log.info("host: $host" )
        log.info("port: $port" )
        log.info("adresses: $server" )
        log.info("jwt: $jwt" )
        log.info("seed: $seed" )
        val token = Nats.staticCredentials(jwt.toCharArray(), seed.toCharArray())
        return  Nats.connect(
            Options.Builder().apply {
                if (host.isNullOrEmpty()) {
                    server(server)
                } else if (port.isNullOrEmpty()) {
                    server("nats://$host:4222")
                } else {
                    server("nats://$host:$port")
                }
                if (!jwt.isNullOrEmpty() && !seed.isNullOrEmpty()) {
                    authHandler(Nats.staticCredentials(jwt.toCharArray(), seed.toCharArray()))
                }
                connectionName(name)
                this.noEcho()
            }.build()
        ).also {
            log.info("connected to nats: ${it.connectedUrl}; status: ${it.status}")
        }
    }

    fun jetStream(conn: Connection): JetStream {
        return conn.jetStream(
            JetStreamOptions.builder().apply {
                requestTimeout(Duration.ofSeconds(2))
                publishNoAck(false) // messages must be ack
            }.build()
        ).also {
            log.info("Called NatsConnectionFactory.jetStream: ${it}")
        }
    }

    fun jetStreamManagement(conn: Connection): JetStreamManagement? {
        return conn.jetStreamManagement(
            JetStreamOptions.builder().apply {
                requestTimeout(Duration.ofSeconds(2))
                publishNoAck(false) // messages must be ack
            }.build()
        ).also {
            log.info("Called NatsConnectionFactory.jetStreamManagement: ${it}")
        }
    }
}

fun JetStreamManagement.createStream(
    name: String,
    retention: RetentionPolicy = RetentionPolicy.WorkQueue,
    storageType: StorageType = StorageType.File,
    vararg subjects: String,
): StreamInfo? {
    return this.addStream(
        StreamConfiguration.builder()
            .name(name)
            .retentionPolicy(retention)
            .subjects(*subjects)
            .storageType(storageType)
            .maxAge(Duration.ofDays(30))
            .build()
    )
}
fun JetStreamManagement.deleteStreamIfExists(streamName: String){
    kotlin.runCatching {
        this.getConsumerNames(streamName).forEach {
            this.deleteConsumer(streamName,it)
        }
    }
    kotlin.runCatching {
        this.deleteStream(streamName)
    }
}

fun JetStreamManagement.logStreamsAndConsumers(header: String){
    val log = LoggerFactory.getLogger(this.javaClass)
    log.info(header)
    log.info("Total Streams : ${streams.size}")
    streams.forEach { streamInfo ->
        log.info(streamInfo.toString())
        this.getConsumers(streamInfo.configuration.name).forEach{ consumerInfo ->
            log.info(consumerInfo.toString())
        }
    }
}