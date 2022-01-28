package fi.hsl.transitdata.eke_sink

import fi.hsl.common.config.ConfigParser
import fi.hsl.common.pulsar.PulsarApplication
import fi.hsl.transitdata.eke_sink.azure.BlobUploader
import fi.hsl.transitdata.eke_sink.sink.AzureSink
import fi.hsl.transitdata.eke_sink.sink.LocalSink
import fi.hsl.transitdata.eke_sink.sink.Sink
import fi.hsl.transitdata.eke_sink.utils.DaemonThreadFactory
import mu.KotlinLogging
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import java.time.Duration
import java.time.LocalDateTime
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

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

    val readyToUpload = mutableSetOf<Path>()

    fun addToUploadList(path: Path) = synchronized(readyToUpload) { readyToUpload.add(path) }

    fun getReadyToUploadCopy(): List<Path> = synchronized(readyToUpload) { readyToUpload.toList() }

    fun removeFromUploadList(path: Path) = synchronized(readyToUpload) { readyToUpload.remove(path) }

    try {
        PulsarApplication.newInstance(config).use { app ->
            val context = app.context

            val messageHandler = MessageHandler(context, dataDirectory, ::addToUploadList)

            val sinkType = config.getString("application.sink")
            val sink = if (sinkType == "local") {
                val sinkDirectory = Files.createTempDirectory("eke")
                log.info { "Using local sink for copying files to local filesystem (${sinkDirectory.toAbsolutePath()})" }
                //TODO: these parameters could be configurable
                LocalSink(sinkDirectory, 50.toDuration(DurationUnit.MILLISECONDS), 50)
            } else {
                log.info { "Using Azure sink for uploading files to Blob Storage" }
                AzureSink(BlobUploader(config.getString("application.blobConnectionString"), config.getString("application.blobContainer")))
            }

            setupTaskToMoveFiles(
                ::getReadyToUploadCopy,
                ::removeFromUploadList,
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
private fun setupTaskToMoveFiles(getReadyToUploadCopy: () -> List<Path>, removeFromUploadList: (Path) -> Unit, sink: Sink, messageHandler: MessageHandler){
    val scheduler = Executors.newSingleThreadScheduledExecutor(DaemonThreadFactory)

    val now = LocalDateTime.now()
    var initialUploadTime = now.withMinute(45)
    if (initialUploadTime.isBefore(now)) {
        initialUploadTime = initialUploadTime.plusHours(1)
    }
    val initialDelay = Duration.between(now, initialUploadTime)

    scheduler.scheduleAtFixedRate({
        try {
            val readyForUpload = getReadyToUploadCopy()
            log.info{ "Starting to upload files to Blob Storage. Number of files to upload: ${readyForUpload.size}" }

            readyForUpload.forEach { file ->
                    if (Files.notExists(file)) {
                        log.warn { "$file does not exist! Skipping upload.." }
                        return@forEach
                    }

                    log.info { "Uploading $file with ${sink::class.simpleName}" }
                    sink.upload(file)
                    log.info { "Uploaded $file" }

                    //Acknowledge messages that were written to the file
                    messageHandler.ackMessages(file)
                    //Delete the file from the disk
                    deleteSafely(file)
                    //Remove file from the upload list so that it won't be uploaded again
                    removeFromUploadList(file)
                }

            log.info("Done to uploading files to blob")
        } catch(t : Throwable) {
            log.error("Something went wrong while moving the files to blob", t)
        }
    }, initialDelay.toMinutes() ,60, TimeUnit.MINUTES)
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