package com.nomad.example.wsservice

import com.fasterxml.jackson.databind.ObjectMapper
import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.SpringApplication
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.integration.channel.PublishSubscribeChannel
import org.springframework.integration.dsl.IntegrationFlows
import org.springframework.integration.file.dsl.Files
import org.springframework.messaging.Message
import org.springframework.messaging.MessageHandler
import org.springframework.web.reactive.HandlerMapping
import org.springframework.web.reactive.handler.SimpleUrlHandlerMapping
import org.springframework.web.reactive.socket.WebSocketHandler
import org.springframework.web.reactive.socket.WebSocketMessage
import org.springframework.web.reactive.socket.WebSocketSession
import org.springframework.web.reactive.socket.server.support.WebSocketHandlerAdapter
import reactor.core.publisher.Flux
import reactor.core.publisher.FluxSink
import java.io.File
import java.util.concurrent.ConcurrentHashMap
import java.util.function.Consumer

@SpringBootApplication
class WsServiceApplication

fun main(args: Array<String>) {
    SpringApplication.run(WsServiceApplication::class.java, *args)
}

@Configuration
class WebSocketCongifuration {

    @Bean
    fun incomingFileFlow(@Value("file://\${HOME}/Desktop/in") f: File)  = IntegrationFlows
            .from(Files.inboundAdapter(f).autoCreateDirectory(true),
                    { p -> p.poller({pm -> pm.fixedRate(1000)}) })
            .channel(incomingFilesChannel())
            .get()

    @Bean
    fun incomingFilesChannel() = PublishSubscribeChannel()

    @Bean
    fun wsha() = WebSocketHandlerAdapter()

    @Bean
    fun hm(): HandlerMapping {
        val suhm = SimpleUrlHandlerMapping()
        suhm.order = 10;
        suhm.urlMap = mapOf("/ws/files" to wsh())
        return suhm;
    }


    @Bean
    fun wsh(): WebSocketHandler {
        /*return WebSocketHandler { session ->

            val om = ObjectMapper()
            val publisher = Flux
                    .generate(Consumer<SynchronousSink<FilesEvent>> { sink ->
                        sink.next(FilesEvent("${System.currentTimeMillis()}", "/a/b/c"))
                    })
                    .map { om.writeValueAsString(it) }
                    .map { session.textMessage(it) }
                    .delayElements((Duration.ofSeconds(1L)))
            session.send(publisher)
        }*/

        val om = ObjectMapper()
        val connections = ConcurrentHashMap<String, MessageHandler>()

        class ForwardingMessageHander(val session: WebSocketSession,
                                      val sink : FluxSink<WebSocketMessage>) : MessageHandler {

            private val sessionId = session.id
            override fun handleMessage(msg: Message<*>) {

                val payload = msg.payload as File
                val fe = FilesEvent(sessionId = sessionId, path = payload.absolutePath)
                val str = om.writeValueAsString(fe)
                val tm = session.textMessage(str)
                sink.next(tm)
                //do nothing
            }
        }

        return WebSocketHandler { session ->

            println("#### session : " + session)
            val publisher = Flux
                    .create(Consumer<FluxSink<WebSocketMessage>> { sink ->
                        //todo
                        // tregister the connection to the client
                        // connect to the client

                        connections[session.id] = ForwardingMessageHander(session,sink)
                        incomingFilesChannel().subscribe(connections[session.id])

                    })
                    .doFinally {
                        incomingFilesChannel().unsubscribe(connections[session.id])
                        connections.remove(session.id)
                    }
//            session.receive()
            session.send(publisher)
        }
    }
}

data class FilesEvent(val sessionId: String, val path: String)

