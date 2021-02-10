package fi.hsl.transitdata.eke_sink

import fi.hsl.common.config.ConfigParser
import fi.hsl.common.config.ConfigUtils
import fi.hsl.common.pulsar.PulsarApplication
import fi.hsl.transitdata.eke_sink.azure.AzureBlobClient
import fi.hsl.transitdata.eke_sink.azure.AzureUploader
import liquibase.Contexts
import liquibase.LabelExpression
import liquibase.Liquibase
import liquibase.database.DatabaseFactory
import liquibase.database.jvm.JdbcConnection
import liquibase.resource.ClassLoaderResourceAccessor
import liquibase.resource.FileSystemResourceAccessor
import mu.KotlinLogging
import okhttp3.OkHttpClient
import java.io.File
import java.sql.DriverManager
import java.time.Duration
import java.time.LocalDateTime
import java.time.ZoneId
import java.util.*
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit


const val PATH = "json"
val log = KotlinLogging.logger {}
fun main(vararg args: String) {


    val config = ConfigParser.createConfig()

    val client = OkHttpClient()
    val path : File = File(PATH)
    if(!path.exists()) path.mkdir()
    try {
        PulsarApplication.newInstance(config).use { app ->
            val context = app.context
            val messageHandler = MessageHandler(context, path)
            setupTaskToMoveFiles(context.config!!.getString("application.blobConnectionString"),
                context.config!!.getString("application.blobContainer"), messageHandler)
            app.launchWithHandler(messageHandler)
        }
    } catch (e: Exception) {
        log.error("Exception at main", e)
    }
}

/**
 * Moves the files from the local storage to a shared azure blob
 */
fun setupTaskToMoveFiles(blobConnectionString : String, blobContainer : String, messageHandler: MessageHandler){
    val scheduler = Executors.newScheduledThreadPool(1)
    val tomorrow = LocalDateTime.now().plusDays(1).withHour(3).atZone(ZoneId.of("Europe/Helsinki"))
    val now = LocalDateTime.now()
    val initialDelay = Duration.between(now, tomorrow)
    scheduler.scheduleWithFixedDelay(Runnable {
        try{
            log.info("Starting to move files to blob")
            File(PATH).list()!!.forEach {
                val file = File(PATH, it)
                val azureBlobClient = AzureBlobClient(blobConnectionString, blobContainer)
                val uploader = AzureUploader(azureBlobClient)
                uploader.uploadBlob(file)
                file.delete()
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