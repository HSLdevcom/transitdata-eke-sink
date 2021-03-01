package fi.hsl.transitdata.eke_sink

import fi.hsl.common.mqtt.proto.Mqtt
import fi.hsl.common.pulsar.IMessageHandler
import fi.hsl.common.pulsar.PulsarApplicationContext
import fi.hsl.common.transitdata.TransitdataProperties
import fi.hsl.common.transitdata.proto.Eke
import fi.hsl.transitdata.eke_sink.EkeBinaryParser.EKE_TIME
import fi.hsl.transitdata.eke_sink.EkeBinaryParser.TIME
import fi.hsl.transitdata.eke_sink.EkeBinaryParser.TRAIN_NUMBER
import mu.KotlinLogging
import org.apache.commons.csv.CSVFormat
import org.apache.commons.csv.CSVPrinter
import org.apache.pulsar.client.api.Consumer
import org.apache.pulsar.client.api.Message
import org.apache.pulsar.client.api.MessageId
import org.apache.pulsar.client.api.Producer
import java.io.File
import java.text.SimpleDateFormat
import java.util.*

import java.io.BufferedWriter
import java.io.FileWriter
import java.nio.file.Files
import java.nio.file.StandardOpenOption

const val TOPIC_PREFIX = "eke/v1/sm5/"

class MessageHandler(val context: PulsarApplicationContext, private val path : File, private val outputFormat : String) : IMessageHandler {

    private val log = KotlinLogging.logger {}
    private val JSON_FILE_NAME_PATTERN = "day_%s_unit_%s.json"
    private val CSV_FILE_NAME_PATTERN = "day_%s_unit_%s.csv"
    private val consumer: Consumer<ByteArray> = context.consumer!!
    private val sdfDayHour : SimpleDateFormat = SimpleDateFormat("dd-MM-yyyy-HH")
    private var lastHandledMessage : MessageId? = null
    private var handledMessages = 0
    private val producer : Producer<ByteArray> = context.singleProducer!!

    override fun handleMessage(received: Message<Any>) {
        try {
            val data: ByteArray = received.data
            val raw = Mqtt.RawMessage.parseFrom(data)
            val rawPayload = raw.payload.toByteArray()
            if(outputFormat == "csv"){
                writeToCSVFile(rawPayload, raw.topic)
            }
            else{
                writeToJsonFile(rawPayload, raw.topic)
            }
            val messageSummary = Eke.EkeSummary.newBuilder()
                .setEkeDate(rawPayload.readField(EKE_TIME).toInstant().toEpochMilli())
                .setTrainNumber(rawPayload.readField(TRAIN_NUMBER))
                .setTopicPart(raw.topic)
                .build()
            sendMessageSummary(messageSummary)
            handledMessages++
            if(handledMessages == 100000){
                log.info("Handled 100000 messages, everything seems fine")
                handledMessages = 0
            }
            lastHandledMessage = received.messageId
        } catch (e: Exception) {
            log.error("Exception while handling message", e)
        }
    }

    fun ackMessages() {
        if(lastHandledMessage != null){
            consumer.acknowledgeAsync(lastHandledMessage)
                .exceptionally { throwable ->
                    log.error("Failed to ack Pulsar messages", throwable)
                    null
                }
                .thenRun {}
        }
    }

    fun sendMessageSummary(messageSummary : Eke.EkeSummary){
        producer.newMessage()
            .property(TransitdataProperties.KEY_PROTOBUF_SCHEMA, TransitdataProperties.ProtobufSchema.EkeSummary.toString())
            .value(messageSummary.toByteArray())
            .sendAsync()
    }

    private fun getUnitNumber(topic : String) : String{
        return topic.replace(TOPIC_PREFIX,"").split("/")[0]
    }



    private fun writeToCSVFile(payload: ByteArray, topic : String){
        val date = payload.readField(EKE_TIME)
        val file = File(path, String.format(CSV_FILE_NAME_PATTERN, sdfDayHour.format(date), getUnitNumber(topic)))
        val csvPrinter = if(file.exists()){
            CSVPrinter(FileWriter(file, true), CSVFormat.DEFAULT.withDelimiter(";".single()))
        }
        else{
            CSVPrinter(FileWriter(file, false), CSVFormat.DEFAULT.withDelimiter(";".single()).withHeader(*EkeBinaryParser.fields.map {it.jsonFieldName }.toTypedArray(), "topic"))
        }
        csvPrinter.use {
            csvPrinter.printRecord(*EkeBinaryParser.fields.map {it.toString(payload) }.toTypedArray(), topic)
            csvPrinter.flush()
        }
    }

    private fun writeToJsonFile(payload: ByteArray, topic : String){
        val date = payload.readField(EKE_TIME)
        val apcJson = EkeBinaryParser.toJson(payload, topic)
        val file = File(path, String.format(JSON_FILE_NAME_PATTERN, sdfDayHour.format(date), getUnitNumber(topic)))
        if(!file.exists()) file.createNewFile()
        file.appendText("${apcJson}\n")
    }
}