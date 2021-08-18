package fi.hsl.transitdata.eke_sink

import fi.hsl.common.mqtt.proto.Mqtt
import fi.hsl.common.pulsar.IMessageHandler
import fi.hsl.common.pulsar.PulsarApplicationContext
import fi.hsl.common.transitdata.TransitdataProperties
import fi.hsl.common.transitdata.proto.Eke
import fi.hsl.transitdata.eke_sink.messages.ConnectionStatus
import fi.hsl.transitdata.eke_sink.messages.MqttHeader
import mu.KotlinLogging
import org.apache.commons.codec.binary.Hex
import org.apache.pulsar.client.api.Consumer
import org.apache.pulsar.client.api.Message
import org.apache.pulsar.client.api.MessageId
import org.apache.pulsar.client.api.Producer

import java.nio.file.Path
import java.time.*
import java.time.format.DateTimeFormatter

const val TOPIC_PREFIX = "eke/v1/sm5/"

class MessageHandler(context: PulsarApplicationContext, fileDirectory: Path) : IMessageHandler {
    private val log = KotlinLogging.logger {}

    private val consumer: Consumer<ByteArray> = context.consumer!!

    private var lastHandledMessage : MessageId? = null
    private var handledMessages = 0

    private val producer : Producer<ByteArray> = context.singleProducer!!

    private val csvHelper = CSVHelper(fileDirectory, Duration.ofMinutes(30), listOf("message_type", "ntp_timestamp", "ntp_ok", "eke_timestamp", "mqtt_timestamp", "mqtt_topic", "raw_data"))

    override fun handleMessage(received: Message<Any>) {
        try {
            val data: ByteArray = received.data
            //Time when the MQTT message was received (nullable because older versions of mqtt-pulsar-gateway don't support this)
            val mqttTimestamp = received.properties[TransitdataProperties.KEY_SOURCE_MESSAGE_TIMESTAMP_MS]?.toLong()?.let { epochMilli -> Instant.ofEpochMilli(epochMilli) }

            val raw = Mqtt.RawMessage.parseFrom(data)
            val rawPayload = raw.payload.toByteArray()
            
            writeToCSVFile(rawPayload, raw.topic, mqttTimestamp)

            if (raw.topic.endsWith("stadlerUDP")) {
                val header = MqttHeader.parseFromByteArray(rawPayload)

                val messageSummary = Eke.EkeSummary.newBuilder()
                    .setEkeDate(header.ekeTimeExact.toInstant().toEpochMilli())
                    .setTrainNumber(getUnitNumber(raw.topic).toInt())
                    .setTopicPart(raw.topic)
                    .build()
                sendMessageSummary(messageSummary)
            }

            if (++handledMessages == 1000) {
                log.info("Handled 1000 messages, everything seems fine")
                handledMessages = 0
            }
            lastHandledMessage = received.messageId
        } catch (e: Exception) {
            log.error("Exception while handling message", e)
        }
    }

    fun ackMessages() {
        if (lastHandledMessage != null){
            //Acknowledge all messages up to and including last received
            //TODO: should we acknowledge messages individually?
            consumer.acknowledgeCumulativeAsync(lastHandledMessage)
                .exceptionally { throwable ->
                    log.error("Failed to ack Pulsar messages", throwable)
                    null
                }
                .thenRun {}
        }
    }

    private fun sendMessageSummary(messageSummary: Eke.EkeSummary){
        producer.newMessage()
            .property(TransitdataProperties.KEY_PROTOBUF_SCHEMA, TransitdataProperties.ProtobufSchema.EkeSummary.toString())
            .value(messageSummary.toByteArray())
            .sendAsync()
    }

    private fun getUnitNumber(topic: String) : String{
        return topic.replace(TOPIC_PREFIX,"").split("/")[0]
    }

    private fun writeToCSVFile(payload: ByteArray, topic: String, mqttReceivedTimestamp: Instant?)  {
        val mqttTime = mqttReceivedTimestamp?.atZone(ZoneId.of("UTC"))

        if (mqttTime == null) {
            log.warn { "No MQTT received timestamp, cannot store message" }
            return
        }

        val mqttTopic = topic
        val mqttTimeString = mqttTime.format(DateTimeFormatter.ISO_OFFSET_DATE_TIME)

        val unitNumber = getUnitNumber(topic)

        if (topic.endsWith("connectionStatus")) {
            val connectionStatus = ConnectionStatus.parseConnectionStatus(payload)

            if (connectionStatus == null) {
                log.warn { "Failed to parse connection status message, topic: $topic, payload: $payload" }
                return
            }

            val csvRecord: List<String> = listOf(
                ConnectionStatus.CONNECTION_STATUS_MESSAGE_TYPE.toString(),
                "", //Connection status message does not have NTP time
                "",
                connectionStatus.timestamp?.format(DateTimeFormatter.ISO_OFFSET_DATE_TIME) ?: "",
                mqttTimeString,
                mqttTopic,
                connectionStatus.status
            )

            csvHelper.writeToCsv(formatCsvFileName(mqttTime.withZoneSameInstant(ZoneId.of("Europe/Helsinki")).toLocalDateTime(), unitNumber), csvRecord)
        } else {
            val mqttHeader = MqttHeader.parseFromByteArray(payload)

            val messageType = mqttHeader.messageType.toString()
            val ntpTime = mqttHeader.ntpTimeExact.format(DateTimeFormatter.ISO_OFFSET_DATE_TIME)
            val ntpOk = mqttHeader.ntpValid.toString()
            val ekeTime = mqttHeader.ekeTimeExact.format(DateTimeFormatter.ISO_OFFSET_DATE_TIME)
            //Encode raw message to hex string so that it can be added to CSV
            val rawData = Hex.encodeHexString(payload)

            val csvRecord: List<String> = listOf(messageType, ntpTime, ntpOk, ekeTime, mqttTimeString, mqttTopic, rawData)

            csvHelper.writeToCsv(formatCsvFileName(mqttHeader.ekeTimeExact.toLocalDateTime(), unitNumber), csvRecord)
        }
    }
}