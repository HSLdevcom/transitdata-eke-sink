package fi.hsl.transitdata.eke_sink

import fi.hsl.common.config.ConfigParser
import fi.hsl.common.pulsar.PulsarApplication
import fi.hsl.transitdata.eke_sink.azure.AzureBlobClient
import fi.hsl.transitdata.eke_sink.azure.AzureUploader
import mu.KotlinLogging
import java.io.File
import java.nio.file.Files
import java.nio.file.Paths
import java.time.Duration
import java.time.LocalDateTime
import java.time.ZoneId
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

import java.time.format.DateTimeFormatter


private val PATH = Paths.get("eke")
private val log = KotlinLogging.logger {}

fun main(vararg args: String) {
    val config = ConfigParser.createConfig()

    if(!Files.exists(PATH)) Files.createDirectories(PATH)

    try {
        PulsarApplication.newInstance(config).use { app ->
            val context = app.context

            val messageHandler = MessageHandler(context, PATH, config.getString("application.outputformat"))

            setupTaskToMoveFiles(
                config.getString("application.blobConnectionString"),
                config.getString("application.blobContainer"),
                messageHandler)

            app.launchWithHandler(messageHandler)
        }
    } catch (e: Exception) {
        log.error("Exception at main", e)
    }
}

val sdfDayHour: DateTimeFormatter = DateTimeFormatter.ofPattern ("dd-MM-yyyy-HH")
/**
 * Moves the files from the local storage to a shared azure blob
 */
private fun setupTaskToMoveFiles(blobConnectionString : String, blobContainer : String, messageHandler: MessageHandler){
    val scheduler = Executors.newScheduledThreadPool(1)
    val nextHour = LocalDateTime.now().plusHours(1).withMinute(5).atZone(ZoneId.of("Europe/Helsinki"))
    val now = LocalDateTime.now()
    val initialDelay = Duration.between(now, nextHour)
    scheduler.scheduleWithFixedDelay({
        try {
            val boundaryForPastData = LocalDateTime.now().minusHours(1).withMinute(0).atZone(ZoneId.of("Europe/Helsinki"))
            log.info("Starting to upload files to Blob Storage")
            //Collect trains
            val allFiles = Files.list(PATH)!!
            //List of the unit number of all the vehicles which have a file
            allFiles
                .filter {
                    !LocalDateTime.parse(it.fileName.toString().split("_unit_")[0].split("_day_")[1], sdfDayHour).isAfter(boundaryForPastData.toLocalDateTime())
                }
                .forEach { uncompressed ->
                    //GZIP file and upload to Azure Blob Storage
                    log.debug { "Compressing $uncompressed with GZIP" }
                    val compressed = gzip(uncompressed)
                    log.debug { "Compressed $uncompressed to $compressed" }

                    val azureBlobClient = AzureBlobClient(blobConnectionString, blobContainer)
                    val uploader = AzureUploader(azureBlobClient)
                    uploader.uploadBlob(compressed.toFile())

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