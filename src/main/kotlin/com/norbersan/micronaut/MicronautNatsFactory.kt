package com.norbersan.micronaut

import com.norbersan.common.NatsConnectionFactory
import io.micronaut.context.annotation.Factory
import io.micronaut.context.annotation.Property
import io.nats.client.Connection
import jakarta.inject.Singleton

@Factory
class MicronautNatsFactory {
    private val natsConnectionFactory = NatsConnectionFactory()

    @Singleton
    fun getConnection(
        @Property(name = "nats.host", defaultValue = "")
        host: String,
        @Property(name = "nats.port", defaultValue = "")
        port: String,
        @Property(name = "nats.server", defaultValue = "")
        server: String,
        @Property(name = "nats.servers", defaultValue = "")
        vararg servers: String,
        @Property(name = "nats.jwt", defaultValue = "")
        jwt: String,
        @Property(name = "nats.seed", defaultValue = "")
        seed: String
    ): Connection =
        natsConnectionFactory.getConnection(
            host = host,
            port = port,
            server = server,
            servers = servers,
            jwt = jwt,
            seed = seed
        )
}