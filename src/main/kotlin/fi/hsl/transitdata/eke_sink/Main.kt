package fi.hsl.transitdata.eke_sink

import fi.hsl.common.config.ConfigParser
import fi.hsl.common.config.ConfigUtils
import fi.hsl.common.pulsar.PulsarApplication
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
import java.util.*


fun main(vararg args: String) {
    val log = KotlinLogging.logger {}

    val config = ConfigParser.createConfig()

    val client = OkHttpClient()

    //Default path is what works with Docker out-of-the-box. Override with a local file if needed
    val secretFilePath = ConfigUtils.getEnv("FILEPATH_CONNECTION_STRING").orElse("/run/secrets/pubtrans_community_conn_string")
    val connectionString = Scanner(File(secretFilePath))
            .useDelimiter("\\Z").next()

    try {
        DriverManager.getConnection(connectionString).use { connection ->
            val database = DatabaseFactory.getInstance().findCorrectDatabaseImplementation(JdbcConnection(connection))
            val liquibase = Liquibase("migrations.xml", ClassLoaderResourceAccessor(), database)
            liquibase.update(Contexts(), LabelExpression())
            PulsarApplication.newInstance(config).use { app ->
                val dbWriter = EkeMessageDbWriter(connection)
                val context = app.context
                val processor = MessageHandler(context, dbWriter)
                val healthServer = context.healthServer
                app.launchWithHandler(processor)
            }
        }
    } catch (e: Exception) {
        log.error("Exception at main", e)
    }
}