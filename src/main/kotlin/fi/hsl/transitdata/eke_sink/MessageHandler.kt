package fi.hsl.transitdata.eke_sink

import fi.hsl.common.mqtt.proto.Mqtt
import fi.hsl.common.pulsar.IMessageHandler
import fi.hsl.common.pulsar.PulsarApplicationContext
import org.apache.pulsar.client.api.Message

class MessageHandler(val context: PulsarApplicationContext, val dbWriter: EkeMessageDbWriter) : IMessageHandler {

    override fun handleMessage(received: Message<Any>) {
        val timestamp: Long = received.eventTime
        val data: ByteArray = received.data
        val raw = Mqtt.RawMessage.parseFrom(data)
        val rawPayload = raw.payload.toByteArray()
        dbWriter.writeMessageToDb(rawPayload)
        //val payload = EkeBinaryParser.toMessageDTO(rawPayload)

    }
}