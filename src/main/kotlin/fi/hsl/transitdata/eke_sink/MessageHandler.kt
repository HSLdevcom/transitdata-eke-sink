package fi.hsl.transitdata.eke_sink

import fi.hsl.common.mqtt.proto.Mqtt
import fi.hsl.common.pulsar.IMessageHandler
import fi.hsl.common.transitdata.TransitdataProperties
import mu.KotlinLogging
import org.apache.pulsar.client.api.Message

import java.time.*

private const val LOG_THRESHOLD = 10000

class MessageHandler(private val csvService: CsvService) : IMessageHandler {
    private val log = KotlinLogging.logger {}

    private var handledMessages = 0

    override fun handleMessage(received: Message<Any>) {
        try {
            val data: ByteArray = received.data
            //Time when the MQTT message was received (nullable because older versions of mqtt-pulsar-gateway don't support this)
            val mqttTimestamp = received.properties[TransitdataProperties.KEY_SOURCE_MESSAGE_TIMESTAMP_MS]?.toLong()?.let { epochMilli -> Instant.ofEpochMilli(epochMilli) }

            val raw = Mqtt.RawMessage.parseFrom(data)
            val rawPayload = raw.payload.toByteArray()

            csvService.writeToCsvFile(raw.topic, rawPayload, mqttTimestamp, received.messageId)

            if (++handledMessages == LOG_THRESHOLD) {
                log.info("Handled $LOG_THRESHOLD messages. Free memory: ${Runtime.getRuntime().freeMemory() / 1024 / 1024}MB")
                handledMessages = 0
            }
        } catch (e: Exception) {
            log.error("Exception while handling message", e)
        }
    }
}