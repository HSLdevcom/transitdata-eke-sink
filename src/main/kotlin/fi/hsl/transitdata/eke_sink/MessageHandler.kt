package fi.hsl.transitdata.eke_sink

import fi.hsl.common.mqtt.proto.Mqtt
import fi.hsl.common.pulsar.IMessageHandler
import fi.hsl.common.pulsar.PulsarApplicationContext
import mu.KotlinLogging
import org.apache.pulsar.client.api.Consumer
import org.apache.pulsar.client.api.Message
import org.apache.pulsar.client.api.MessageId

class MessageHandler(val context: PulsarApplicationContext, val dbWriter: EkeMessageDbWriter) : IMessageHandler {

    private val log = KotlinLogging.logger {}

    private val consumer: Consumer<ByteArray> = context.consumer!!

    override fun handleMessage(received: Message<Any>) {
        val timestamp: Long = received.eventTime
        val data: ByteArray = received.data
        val raw = Mqtt.RawMessage.parseFrom(data)
        val rawPayload = raw.payload.toByteArray()
        dbWriter.writeMessageToDb(rawPayload)
        ack(received.messageId)
    }

    private fun ack(received: MessageId) {
        consumer.acknowledgeAsync(received)
                .exceptionally { throwable ->
                    log.error("Failed to ack Pulsar message", throwable)
                    null
                }
                .thenRun {}
    }
}