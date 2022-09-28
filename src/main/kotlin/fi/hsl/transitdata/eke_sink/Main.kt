package fi.hsl.transitdata.eke_sink

import fi.hsl.common.config.ConfigParser
import fi.hsl.common.pulsar.PulsarApplication
import fi.hsl.transitdata.eke_sink.azure.BlobUploader
import fi.hsl.transitdata.eke_sink.sink.AzureSink
import fi.hsl.transitdata.eke_sink.sink.LocalSink
import mu.KotlinLogging
import org.apache.pulsar.client.api.MessageId
import java.nio.file.Files
import java.nio.file.Paths
import java.time.Duration

import kotlin.time.DurationUnit
import kotlin.time.ExperimentalTime
import kotlin.time.toDuration

private val log = KotlinLogging.logger {}

@ExperimentalTime
fun main(vararg args: String) {
    val config = ConfigParser.createConfig()

    val dataDirectory = Paths.get("eke")
    if (!Files.exists(dataDirectory)) {
        Files.createDirectories(dataDirectory)
    }

    try {
        PulsarApplication.newInstance(config).use { app ->
            val context = app.context

            val sinkType = config.getString("application.sink")
            val sink = if (sinkType == "local") {
                val path = config.getString("application.localSinkPath")

                val sinkDirectory = if (path.isNullOrBlank()) { Files.createTempDirectory("eke") } else { Paths.get(path) }
                log.info { "Using local sink for copying files to local filesystem (${sinkDirectory.toAbsolutePath()})" }
                //TODO: these parameters could be configurable
                LocalSink(sinkDirectory, 50.toDuration(DurationUnit.MILLISECONDS), 50)
            } else {
                log.info { "Using Azure sink for uploading files to Blob Storage" }
                AzureSink(BlobUploader(config.getString("application.blobConnectionString"), config.getString("application.blobContainer")))
            }

            val uploadAfterNotModified = config.getDuration("application.uploadAfterNotModified")

            fun ack(msgId: MessageId) {
                context.consumer!!.acknowledgeAsync(msgId)
                    .exceptionally { throwable ->
                        log.error("Failed to ack Pulsar message", throwable)
                        null
                    }
                    .thenRun {}
            }
            val csvService = CsvService(dataDirectory, sink, ::ack, uploadAfterNotModified)

            val messageHandler = MessageHandler(csvService)

            app.launchWithHandler(messageHandler)
        }
    } catch (e: Exception) {
        log.error("Exception at main", e)
    }
}