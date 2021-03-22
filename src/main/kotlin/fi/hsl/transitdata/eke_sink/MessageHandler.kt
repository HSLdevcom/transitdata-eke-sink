package fi.hsl.transitdata.eke_sink

import fi.hsl.common.mqtt.proto.Mqtt
import fi.hsl.common.pulsar.IMessageHandler
import fi.hsl.common.pulsar.PulsarApplicationContext
import fi.hsl.common.transitdata.TransitdataProperties
import fi.hsl.common.transitdata.proto.Eke
import fi.hsl.transitdata.eke_sink.converters.FieldDefinition
import fi.hsl.transitdata.eke_sink.converters.readField
import fi.hsl.transitdata.eke_sink.messages.id_struct.IdStructParser
import fi.hsl.transitdata.eke_sink.messages.mqtt_header.EKE_TIME
import fi.hsl.transitdata.eke_sink.messages.stadlerUDP.StadlerUDPParser
import fi.hsl.transitdata.eke_sink.messages.stadlerUDP.StadlerUDPParser.TRAIN_NUMBER
import fi.hsl.transitdata.eke_sink.messages.rmm_io_struct.RmmIoStructParser
import fi.hsl.transitdata.eke_sink.messages.rmm_jkv_struct.RmmJkvStructParser
import fi.hsl.transitdata.eke_sink.messages.rmm_jkv_train_msg.RmmJkvTrainMsgParser
import fi.hsl.transitdata.eke_sink.messages.rmm_painerajavirhe12.RmmPaineRajaVirhe12
import fi.hsl.transitdata.eke_sink.messages.rmm_time_changed_data.RmmTimeChangedDataParser
import mu.KotlinLogging
import org.apache.commons.csv.CSVFormat
import org.apache.commons.csv.CSVPrinter
import org.apache.pulsar.client.api.Consumer
import org.apache.pulsar.client.api.Message
import org.apache.pulsar.client.api.MessageId
import org.apache.pulsar.client.api.Producer
import java.io.File
import java.text.SimpleDateFormat

import java.io.FileWriter

const val TOPIC_PREFIX = "eke/v1/sm5/"

class MessageHandler(val context: PulsarApplicationContext, private val path : File, private val outputFormat : String) : IMessageHandler {

    private val log = KotlinLogging.logger {}
    private val consumer: Consumer<ByteArray> = context.consumer!!
    private val sdfDayHour : SimpleDateFormat = SimpleDateFormat("dd-MM-yyyy-HH")
    private var lastHandledMessage : MessageId? = null
    private var handledMessages = 0
    private val producer : Producer<ByteArray> = context.singleProducer!!
    private val fieldDefinitions : Map<String, Array<FieldDefinition<out Any>>>  = mapOf(
        "idStruct" to IdStructParser.fields,
        "rmmIoStruct" to RmmIoStructParser.fields,
        "rmmJkvStruct" to RmmJkvStructParser.fields,
        //"rmmJkvBeacon" to RmmJkvBeaconParser.fields,
        "rmmJkvTrainMsg" to RmmJkvTrainMsgParser.fields,
        "rmmTimeChangedData" to RmmTimeChangedDataParser.fields,
        "stadlerUDP" to StadlerUDPParser.fields,
        "rmmPaineRajaVirhe12" to RmmPaineRajaVirhe12.fields
    )


    override fun handleMessage(received: Message<Any>) {
        try {
            val data: ByteArray = received.data
            val raw = Mqtt.RawMessage.parseFrom(data)
            val rawPayload = raw.payload.toByteArray()
            var messageIsValid : Boolean = true
            val messageType = raw.topic.split("/")[raw.topic.split("/").size - 1]
            val fields = fieldDefinitions[messageType]
            if(fields != null){
                when(outputFormat) {
                    "csv" -> writeToCSVFile(rawPayload, raw.topic, fields)
                    "json" -> writeToJsonFile(rawPayload, raw.topic)
                }
            }
            else{
                log.error("unknown topic ${raw.topic.split("/")}, ignoring")
                messageIsValid = false
            }

            if(messageIsValid && messageType == "stadlerUDP"){
                val messageSummary = Eke.EkeSummary.newBuilder()
                    .setEkeDate(rawPayload.readField(EKE_TIME).toInstant().toEpochMilli())
                    .setTrainNumber(rawPayload.readField(TRAIN_NUMBER))
                    .setTopicPart(raw.topic)
                    .build()
                sendMessageSummary(messageSummary)
            }
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



    private fun writeToCSVFile(payload: ByteArray, topic : String, fields : Array<FieldDefinition<out Any>>){
        val date = payload.readField(EKE_TIME)
        val messageType = topic.split("/")[topic.split("/").size - 1]
        val file = File(path, String.format(CSV_FILE_NAME_PATTERN, messageType, sdfDayHour.format(date), getUnitNumber(topic)))
        val csvPrinter = if(file.exists()){
            CSVPrinter(FileWriter(file, true), CSVFormat.DEFAULT.withDelimiter(";".single()))
        }
        else{
            CSVPrinter(FileWriter(file, false), CSVFormat.DEFAULT.withDelimiter(";".single()).withHeader(*fields.map {it.fieldName }.toTypedArray()))
        }
        csvPrinter.use {
            csvPrinter.printRecord(*fields.map {it.toString(payload) }.toTypedArray())
            csvPrinter.flush()
        }
    }

    private fun writeToJsonFile(payload: ByteArray, topic : String){
        val date = payload.readField(EKE_TIME)
        val apcJson = StadlerUDPParser.toJson(payload, topic)
        val file = File(path, String.format(JSON_FILE_NAME_PATTERN, topic, sdfDayHour.format(date), getUnitNumber(topic)))
        if(!file.exists()) file.createNewFile()
        file.appendText("${apcJson}\n")
    }
}