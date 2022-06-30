package fi.hsl.transitdata.eke_sink

import fi.hsl.transitdata.eke_sink.csv.CsvFile
import fi.hsl.transitdata.eke_sink.csv.CsvRow
import fi.hsl.transitdata.eke_sink.messages.ConnectionStatus
import fi.hsl.transitdata.eke_sink.messages.MqttHeader
import fi.hsl.transitdata.eke_sink.sink.Sink
import fi.hsl.transitdata.eke_sink.utils.DaemonThreadFactory
import mu.KotlinLogging
import org.apache.commons.codec.binary.Hex
import org.apache.pulsar.client.api.MessageId
import java.nio.file.Files
import java.nio.file.Path
import java.time.Duration
import java.time.Instant
import java.time.LocalDateTime
import java.time.ZoneId
import java.util.LinkedList
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

private val log = KotlinLogging.logger {}

class CsvService(
    private val fileDirectory: Path,
    sink: Sink,
    private val msgAcknowledger: (MessageId) -> Unit,
    uploadAfterNotModified: Duration,
    tryUploadInterval: Duration = minOf(Duration.ofMinutes(5), uploadAfterNotModified.multipliedBy(2))
) {
    companion object {
        private const val TOPIC_PREFIX = "eke/v1/sm5/"
    }

    private val executor = Executors.newSingleThreadScheduledExecutor(DaemonThreadFactory)

    init {
        executor.scheduleWithFixedDelay({
            val readyForUpload = csvFiles.filterValues { csvFile -> csvFile.getLastModifiedAgo() >= uploadAfterNotModified }

            readyForUpload.values.forEach { it.close() }

            log.info { "Uploading files to blob storage, number of files to upload: ${readyForUpload.size}" }

            for ((key, csvFile) in readyForUpload) {
                //Upload to blob storage
                log.info { "Uploading ${csvFile.path} with ${sink::class.simpleName}" }
                sink.upload(csvFile.path, csvFile.getTags())
                log.info { "Uploaded ${csvFile.path}" }

                //Acknowledge messages
                val msgIds = msgIdsByPath[csvFile.path] ?: emptyList()
                log.info { "Acknowledging ${msgIds.size} messages which were written to ${csvFile.path}" }
                for (msgId in msgIds) {
                    msgAcknowledger(msgId)
                }

                //Delete file
                deleteSafely(csvFile.path)

                csvFiles.remove(key)
                msgIdsByPath.remove(csvFile.path)
            }
        }, tryUploadInterval.seconds, tryUploadInterval.seconds, TimeUnit.SECONDS) //TODO: allow changing these values
    }

    private val csvFiles = mutableMapOf<String, CsvFile>()
    private val msgIdsByPath = mutableMapOf<Path, MutableList<MessageId>>()

    private fun getUnitNumber(topic: String) : String{
        return topic.replace(TOPIC_PREFIX,"").split("/")[0]
    }

    private fun getCsvFile(mqttTopic: String, mqttReceivedAtLocalTime: LocalDateTime): CsvFile {
        val unitNumber = getUnitNumber(mqttTopic)
        val fileNameBase = formatCsvFileName(mqttReceivedAtLocalTime, unitNumber)

        return csvFiles.computeIfAbsent(fileNameBase) { CsvFile(fileDirectory.resolve("$fileNameBase.csv.gz"), unitNumber, CsvRow.CSV_HEADER) }
    }

    fun writeToCsvFile(mqttTopic: String, mqttPayload: ByteArray, mqttReceivedAt: Instant?, messageId: MessageId) {
        if (mqttReceivedAt == null) {
            log.warn { "No MQTT received timestamp, cannot store message" }
            msgAcknowledger(messageId) //Ack invalid message so that we don't receive it again
            return
        }

        val mqttTimeUtc = mqttReceivedAt.atZone(ZoneId.of("UTC"))

        val csvFile = getCsvFile(mqttTopic, mqttTimeUtc.withZoneSameInstant(ZoneId.of("Europe/Helsinki")).toLocalDateTime())

        val csvRow = if (mqttTopic.endsWith("connectionStatus")) {
            val connectionStatus = ConnectionStatus.parseConnectionStatus(mqttPayload)

            if (connectionStatus == null) {
                log.warn { "Failed to parse connection status message, topic: $mqttTopic, payload: ${mqttPayload.decodeToString()}" }
                msgAcknowledger(messageId) //Ack invalid message so that we don't receive it again
                return
            }

            CsvRow.create(mqttTopic, mqttTimeUtc, connectionStatus)
        } else {
            val mqttHeader = MqttHeader.parseFromByteArray(mqttPayload)
            val rawData = Hex.encodeHexString(mqttPayload)

            CsvRow.create(mqttTopic, mqttTimeUtc, mqttHeader, rawData)
        }

        csvFile.writeRow(csvRow)
        msgIdsByPath.computeIfAbsent(csvFile.path) { LinkedList<MessageId>() }.add(messageId)
    }

    /**
     * Deletes file at the specified path without throwing any exceptions. If the file cannot be deleted for some reason, the behaviour of this function is unspecified
     */
    private fun deleteSafely(path: Path) {
        try {
            Files.deleteIfExists(path)
        } catch (e: Exception) {
            //TODO: swallowing exception can cause the disk to be filled up. Maybe add timer to delete files that have not been modified in a long time?
            log.warn(e) { "Failed to delete file $path" }
        }
    }
}