package fi.hsl.transitdata.eke_sink

import fi.hsl.common.config.ConfigParser
import fi.hsl.common.pulsar.PulsarApplication
import fi.hsl.transitdata.eke_sink.azure.AzureBlobClient
import fi.hsl.transitdata.eke_sink.azure.AzureUploader
import mu.KotlinLogging
import okhttp3.OkHttpClient
import java.io.File
import java.time.Duration
import java.time.LocalDateTime
import java.time.ZoneId
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

import java.time.format.DateTimeFormatter


private val PATH = File("eke")
private val log = KotlinLogging.logger {}
fun main(vararg args: String) {


    val config = ConfigParser.createConfig()

    val client = OkHttpClient()
    if(!PATH.exists()) PATH.mkdir()
    try {
        PulsarApplication.newInstance(config).use { app ->
           val context = app.context
           val messageHandler = MessageHandler(context, PATH, context.config!!.getString("application.outputformat"))
            setupTaskToMoveFiles(context.config!!.getString("application.blobConnectionString"),
                context.config!!.getString("application.blobContainer"), messageHandler)
            app.launchWithHandler(messageHandler)
        }
    } catch (e: Exception) {
        log.error("Exception at main", e)
    }
}

val ZIP_FILE_PATTERN = "%s_vehicle_%s.zip"
val sdfDayHour  = DateTimeFormatter.ofPattern ("dd-MM-yyyy-HH")
/**
 * Moves the files from the local storage to a shared azure blob
 */
private fun setupTaskToMoveFiles(blobConnectionString : String, blobContainer : String, messageHandler: MessageHandler){
    val scheduler = Executors.newScheduledThreadPool(1)
    val nextHour = LocalDateTime.now().plusHours(1).withMinute(5).atZone(ZoneId.of("Europe/Helsinki"))
    val now = LocalDateTime.now()
    val initialDelay = Duration.between(now, nextHour)
    scheduler.scheduleWithFixedDelay(Runnable {
        try{
            val boundaryForPastData = LocalDateTime.now().minusHours(1).withMinute(0).atZone(ZoneId.of("Europe/Helsinki"))
            log.info("Starting to move files to blob")
            //Collect trains
            val allFiles = PATH.list()!!
            //List of the unit number of all the vehicles which have a file
            allFiles
                .filter { it -> !LocalDateTime.parse(it.split("_unit_")[0].replace("day_",""), sdfDayHour).isAfter(boundaryForPastData.toLocalDateTime())}                .forEach{
                    //Files to zip for vehicle
                    val split = it.split("_unit_")
                    val zipFile = zipFiles(PATH, String.format(ZIP_FILE_PATTERN, split[0], split[1]), listOf(it))
                    val azureBlobClient = AzureBlobClient(blobConnectionString, blobContainer)
                    val uploader = AzureUploader(azureBlobClient)
                    uploader.uploadBlob(zipFile)
                    File(PATH, it).delete()
                    zipFile.delete()
                }
            log.info("Done to move files to blob")
            messageHandler.ackMessages()
            log.info("Pulsar messages acknowledged")
        }
        catch(t : Throwable){
            log.error("Something went wrong while moving the files to blob", t)
        }
    }, initialDelay.toMinutes() ,60, TimeUnit.MINUTES)
}