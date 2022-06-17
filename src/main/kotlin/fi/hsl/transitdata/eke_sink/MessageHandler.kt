package fi.hsl.transitdata.eke_sink

import fi.hsl.common.mqtt.proto.Mqtt
import fi.hsl.common.pulsar.IMessageHandler
import fi.hsl.common.pulsar.PulsarApplicationContext
import fi.hsl.common.transitdata.TransitdataProperties
import fi.hsl.common.transitdata.proto.Eke
import fi.hsl.transitdata.eke_sink.sink.Sink
import mu.KotlinLogging
import org.apache.pulsar.client.api.Consumer
import org.apache.pulsar.client.api.Message
import org.apache.pulsar.client.api.MessageId
import org.apache.pulsar.client.api.Producer

import java.nio.file.Path
import java.time.*

private const val LOG_THRESHOLD = 10000

class MessageHandler(context: PulsarApplicationContext, private val csvService: CsvService) : IMessageHandler {
    private val log = KotlinLogging.logger {}

    private val consumer: Consumer<ByteArray> = context.consumer!!

    private var handledMessages = 0

    private val producer : Producer<ByteArray>? = if (context.config!!.getBoolean("pulsar.producer.enabled")) { context.singleProducer!! } else { null }

    override fun handleMessage(received: Message<Any>) {
        try {
            val data: ByteArray = received.data
            //Time when the MQTT message was received (nullable because older versions of mqtt-pulsar-gateway don't support this)
            val mqttTimestamp = received.properties[TransitdataProperties.KEY_SOURCE_MESSAGE_TIMESTAMP_MS]?.toLong()?.let { epochMilli -> Instant.ofEpochMilli(epochMilli) }

            val raw = Mqtt.RawMessage.parseFrom(data)
            val rawPayload = raw.payload.toByteArray()

            csvService.writeToCsvFile(raw.topic, rawPayload, mqttTimestamp, received.messageId)

            /*if (raw.topic.endsWith("stadlerUDP")) {
                val header = MqttHeader.parseFromByteArray(rawPayload)

                val messageSummary = Eke.EkeSummary.newBuilder()
                    .setEkeDate(header.ekeTimeExact.toInstant().toEpochMilli())
                    .setTrainNumber(getUnitNumber(raw.topic).toInt())
                    .setTopicPart(raw.topic)
                    .build()
                sendMessageSummary(messageSummary)
            }*/

            if (++handledMessages == LOG_THRESHOLD) {
                log.info("Handled $LOG_THRESHOLD messages. Free memory: ${Runtime.getRuntime().freeMemory() / 1024 / 1024}MB")
                handledMessages = 0
            }
        } catch (e: Exception) {
            log.error("Exception while handling message", e)
        }
    }

    private fun sendMessageSummary(messageSummary: Eke.EkeSummary) {
        //Fails quietly if producer is null (when summary messages are not enabled)
        producer?.newMessage()?.property(TransitdataProperties.KEY_PROTOBUF_SCHEMA, TransitdataProperties.ProtobufSchema.EkeSummary.toString())
            ?.value(messageSummary.toByteArray())?.sendAsync()
    }
}