package fi.hsl.transitdata.eke_sink

import fi.hsl.common.config.ConfigParser
import fi.hsl.common.pulsar.PulsarApplication
import fi.hsl.transitdata.eke_sink.azure.AzureBlobClient
import fi.hsl.transitdata.eke_sink.sink.AzureSink
import fi.hsl.transitdata.eke_sink.sink.LocalSink
import fi.hsl.transitdata.eke_sink.sink.Sink
import fi.hsl.transitdata.eke_sink.utils.DaemonThreadFactory
import mu.KotlinLogging
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import java.time.Duration
import java.time.Instant
import java.time.LocalDateTime
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

import java.time.temporal.ChronoUnit
import kotlin.io.path.absolutePathString
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

            val messageHandler = MessageHandler(context, dataDirectory)

            val sinkType = config.getString("application.sink")
            val sink = if (sinkType == "local") {
                val sinkDirectory = Files.createTempDirectory("eke")
                log.info { "Using local sink for copying files to local filesystem (${sinkDirectory.toAbsolutePath()})" }
                //TODO: these parameters could be configurable
                LocalSink(sinkDirectory, 50.toDuration(DurationUnit.MILLISECONDS), 50)
            } else {
                log.info { "Using Azure sink for uploading files to Blob Storage" }
                AzureSink(AzureBlobClient(config.getString("application.blobConnectionString"), config.getString("application.blobContainer")))
            }

            setupTaskToMoveFiles(
                dataDirectory,
                sink,
                messageHandler)

            app.launchWithHandler(messageHandler)
        }
    } catch (e: Exception) {
        log.error("Exception at main", e)
    }
}

/**
 * Moves the files from the local storage to a shared azure blob
 */
private fun setupTaskToMoveFiles(dataDirectory: Path, sink: Sink, messageHandler: MessageHandler){
    val scheduler = Executors.newSingleThreadScheduledExecutor(DaemonThreadFactory)

    val nextHour = LocalDateTime.now().plusHours(1).withMinute(45)
    val initialDelay = Duration.between(LocalDateTime.now(), nextHour)

    scheduler.scheduleWithFixedDelay({
        try {
            log.info("Starting to upload files to Blob Storage")

            val now = Instant.now()
            //Collect trains
            val allFiles = Files.list(dataDirectory)!!
            //List of the unit number of all the vehicles which have a file
            allFiles
                //Upload files that have not been modified for 90 minutes (i.e. no new data is coming in)
                //TODO: can this cause issues in some cases?
                .filter { Files.getLastModifiedTime(it).toInstant().plus(90, ChronoUnit.MINUTES).isBefore(now) }
                .forEach { uncompressed ->
                    //GZIP file and upload to Azure Blob Storage
                    log.debug { "Compressing $uncompressed with GZIP" }
                    val compressed = gzip(uncompressed)
                    log.debug { "Compressed $uncompressed to $compressed" }

                    log.info { "Uploading $compressed with ${sink::class.simpleName}" }
                    sink.upload(compressed)
                    log.info { "Uploaded $compressed" }

                    //Delete files that have been uploaded to Azure
                    Files.delete(uncompressed)
                    Files.delete(compressed)
                }

            log.info("Done to uploading files to blob")
            messageHandler.ackMessages()
            log.info("Pulsar messages acknowledged")
        } catch(t : Throwable) {
            log.error("Something went wrong while moving the files to blob", t)
        }
    }, initialDelay.toMinutes() ,60, TimeUnit.MINUTES)
}