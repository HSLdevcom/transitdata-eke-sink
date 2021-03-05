package fi.hsl.transitdata.eke_sink

import fi.hsl.common.config.ConfigParser
import fi.hsl.common.config.ConfigUtils
import fi.hsl.common.pulsar.PulsarApplication
import fi.hsl.transitdata.eke_sink.azure.AzureBlobClient
import fi.hsl.transitdata.eke_sink.azure.AzureUploader
import mu.KotlinLogging
import okhttp3.OkHttpClient
import java.io.File
import java.sql.DriverManager
import java.text.SimpleDateFormat
import java.time.Duration
import java.time.LocalDateTime
import java.time.ZoneId
import java.util.*
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.zip.ZipEntry
import java.io.FileInputStream

import java.util.zip.ZipOutputStream

import java.io.FileOutputStream
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

val ZIP_FILE_PATTERN = "day_%s_vehicle_%s"
val sdfDay  = DateTimeFormatter.ofPattern ("dd-MM-yyyy")
/**
 * Moves the files from the local storage to a shared azure blob
 */
private fun setupTaskToMoveFiles(blobConnectionString : String, blobContainer : String, messageHandler: MessageHandler){
    val scheduler = Executors.newScheduledThreadPool(1)
    val yesterday = LocalDateTime.now().minusDays(1).atZone(ZoneId.of("Europe/Helsinki"))
    val tomorrow = LocalDateTime.now().plusDays(1).withHour(3).atZone(ZoneId.of("Europe/Helsinki"))
    val now = LocalDateTime.now()
    val initialDelay = Duration.between(now, tomorrow)
    scheduler.scheduleWithFixedDelay(Runnable {
        try{
            log.info("Starting to move files to blob")
            //Collect trains
            val allFiles = PATH.list()!!
            val vehicleSet = PATH.list()!!.map { it -> it.split("_unit_")[1].split(".")[0] }.toSet()
            vehicleSet.forEach{
                val vehicleId = it
                val filesForVehicle = allFiles.filter { it -> it.contains(vehicleId)}
                val zipFile = zipFiles(PATH, String.format(ZIP_FILE_PATTERN, sdfDay.format(yesterday), vehicleId), filesForVehicle)
                val azureBlobClient = AzureBlobClient(blobConnectionString, blobContainer)
                val uploader = AzureUploader(azureBlobClient)
                uploader.uploadBlob(zipFile)
                filesForVehicle.forEach{
                    File(PATH, it).delete()
                    zipFile.delete()
                }
            }
            log.info("Done to move files to blob")
            messageHandler.ackMessages()
            log.info("Pulsar messages acknowledged")
        }
        catch(t : Throwable){
            log.error("Something went wrong while moving the files to blob", t)
        }
    }, initialDelay.toMinutes() ,24 * 60, TimeUnit.MINUTES)
}