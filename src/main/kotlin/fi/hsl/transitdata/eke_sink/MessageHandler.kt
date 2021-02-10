package fi.hsl.transitdata.eke_sink

import fi.hsl.common.mqtt.proto.Mqtt
import fi.hsl.common.passengercount.proto.PassengerCount
import fi.hsl.common.pulsar.IMessageHandler
import fi.hsl.common.pulsar.PulsarApplicationContext
import fi.hsl.transitdata.eke_sink.EkeBinaryParser.EKE_TIME
import fi.hsl.transitdata.eke_sink.EkeBinaryParser.TRAIN_NUMBER
import mu.KotlinLogging
import org.apache.pulsar.client.api.Consumer
import org.apache.pulsar.client.api.Message
import org.apache.pulsar.client.api.MessageId
import java.io.File
import java.io.FileOutputStream
import java.text.SimpleDateFormat
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.util.*

class MessageHandler(val context: PulsarApplicationContext, private val path : File /*val dbWriter: EkeMessageDbWriter*/) : IMessageHandler {

    private val log = KotlinLogging.logger {}
    private val FILE_NAME_PATTERN = "day_%s_vehicle_%s.json"
    private val consumer: Consumer<ByteArray> = context.consumer!!
    private val sdfDayHour : SimpleDateFormat = SimpleDateFormat("dd-MM-yyyy-hh")
    private var lastHandledMessage : MessageId? = null
    private var handledMessages = 0

    override fun handleMessage(received: Message<Any>) {
        try {
            val data: ByteArray = received.data
            val raw = Mqtt.RawMessage.parseFrom(data)
            val rawPayload = raw.payload.toByteArray()
            val apcJson = EkeBinaryParser.toJson(rawPayload)
            val date = rawPayload.readField(EKE_TIME)
            val vehicle = rawPayload.readField(TRAIN_NUMBER)
            writeToFile(apcJson, date, vehicle)
            handledMessages++
            if(handledMessages == 1000){
                log.info("Handled 1000 messages, everything seems fine")
            }
            lastHandledMessage = received.messageId
        } catch (e: Exception) {
            log.error("Exception while handling message", e)
        }
    }

    public fun ackMessages() {
        if(lastHandledMessage != null){
            consumer.acknowledgeAsync(lastHandledMessage)
                .exceptionally { throwable ->
                    log.error("Failed to ack Pulsar messages", throwable)
                    null
                }
                .thenRun {}
        }
    }


    private fun writeToFile(apcJson : String, date : Date, vehicle : Int){
        val file = File(path, String.format(FILE_NAME_PATTERN, sdfDayHour.format(date), vehicle))
        if(!file.exists()) file.createNewFile()
        file.appendText("${apcJson}\n")
    }
}