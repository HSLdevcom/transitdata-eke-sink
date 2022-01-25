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
import java.util.*

private const val TOPIC_PREFIX = "eke/v1/sm5/"

private const val LOG_THRESHOLD = 10000

class MessageHandler(context: PulsarApplicationContext, fileDirectory: Path, addToUploadList: (Path) -> Unit) : IMessageHandler {
    private val log = KotlinLogging.logger {}

    private val consumer: Consumer<ByteArray> = context.consumer!!

    private var handledMessages = 0

    private val producer : Producer<ByteArray>? = if (context.config!!.getBoolean("pulsar.producer.enabled")) { context.singleProducer!! } else { null }

    private val csvHelper = CSVHelper(fileDirectory, Duration.ofMinutes(30), true, listOf("message_type", "ntp_timestamp", "ntp_ok", "eke_timestamp", "mqtt_timestamp", "mqtt_topic", "raw_data"), addToUploadList)

    private val fileToMsgId = mutableMapOf<Path, MutableList<MessageId>>()
    
    private fun getMsgIdList(path: Path): MutableList<MessageId> = fileToMsgId.computeIfAbsent(path) { LinkedList<MessageId>() }

    override fun handleMessage(received: Message<Any>) {
        try {
            val data: ByteArray = received.data
            //Time when the MQTT message was received (nullable because older versions of mqtt-pulsar-gateway don't support this)
            val mqttTimestamp = received.properties[TransitdataProperties.KEY_SOURCE_MESSAGE_TIMESTAMP_MS]?.toLong()?.let { epochMilli -> Instant.ofEpochMilli(epochMilli) }

            val raw = Mqtt.RawMessage.parseFrom(data)
            val rawPayload = raw.payload.toByteArray()
            
            val file = writeToCSVFile(rawPayload, raw.topic, mqttTimestamp)
            file?.let { getMsgIdList(it).add(received.messageId) }

            if (raw.topic.endsWith("stadlerUDP")) {
                val header = MqttHeader.parseFromByteArray(rawPayload)

                val messageSummary = Eke.EkeSummary.newBuilder()
                    .setEkeDate(header.ekeTimeExact.toInstant().toEpochMilli())
                    .setTrainNumber(getUnitNumber(raw.topic).toInt())
                    .setTopicPart(raw.topic)
                    .build()
                sendMessageSummary(messageSummary)
            }

            if (++handledMessages == LOG_THRESHOLD) {
                log.info("Handled $LOG_THRESHOLD messages. Free memory: ${Runtime.getRuntime().freeMemory() / 1024 / 1024}MB, open files: ${csvHelper.openFiles}")
                handledMessages = 0
            }
        } catch (e: Exception) {
            log.error("Exception while handling message", e)
        }
    }

    /**
     * Acks messages which were written to the specified file
     */
    fun ackMessages(path: Path) {
        val messageIds = getMsgIdList(path)
        log.info { "Acknowledging ${messageIds.size} messages which were written to $path" }
        messageIds.forEach {
            consumer.acknowledgeAsync(it)
                .exceptionally { throwable ->
                    log.error("Failed to ack Pulsar message", throwable)
                    null
                }
                .thenRun {}
        }
        fileToMsgId.remove(path)
    }

    private fun sendMessageSummary(messageSummary: Eke.EkeSummary) {
        //Fails quietly if producer is null (when summary messages are not enabled)
        producer?.newMessage()?.property(TransitdataProperties.KEY_PROTOBUF_SCHEMA, TransitdataProperties.ProtobufSchema.EkeSummary.toString())
            ?.value(messageSummary.toByteArray())?.sendAsync()
    }

    private fun getUnitNumber(topic: String) : String{
        return topic.replace(TOPIC_PREFIX,"").split("/")[0]
    }

    private fun writeToCSVFile(payload: ByteArray, topic: String, mqttReceivedTimestamp: Instant?): Path? {
        val mqttTime = mqttReceivedTimestamp?.atZone(ZoneId.of("UTC"))

        if (mqttTime == null) {
            log.warn { "No MQTT received timestamp, cannot store message" }
            return null
        }

        val mqttTopic = topic
        val mqttTimeString = mqttTime.format(DateTimeFormatter.ISO_OFFSET_DATE_TIME)

        val unitNumber = getUnitNumber(topic)

        val messageReceiveTimeLocal = mqttTime.withZoneSameInstant(ZoneId.of("Europe/Helsinki")).toLocalDateTime()

        if (topic.endsWith("connectionStatus")) {
            val connectionStatus = ConnectionStatus.parseConnectionStatus(payload)

            if (connectionStatus == null) {
                log.warn { "Failed to parse connection status message, topic: $topic, payload: $payload" }
                return null
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

            return csvHelper.writeToCsv(formatCsvFileName(messageReceiveTimeLocal, unitNumber), csvRecord)
        } else {
            val mqttHeader = MqttHeader.parseFromByteArray(payload)

            val messageType = mqttHeader.messageType.toString()
            val ntpTime = mqttHeader.ntpTimeExact.format(DateTimeFormatter.ISO_OFFSET_DATE_TIME)
            val ntpOk = mqttHeader.ntpValid.toString()
            val ekeTime = mqttHeader.ekeTimeExact.format(DateTimeFormatter.ISO_OFFSET_DATE_TIME)
            //Encode raw message to hex string so that it can be added to CSV
            val rawData = Hex.encodeHexString(payload)

            val csvRecord: List<String> = listOf(messageType, ntpTime, ntpOk, ekeTime, mqttTimeString, mqttTopic, rawData)

            return csvHelper.writeToCsv(formatCsvFileName(messageReceiveTimeLocal, unitNumber), csvRecord)
        }
    }
}