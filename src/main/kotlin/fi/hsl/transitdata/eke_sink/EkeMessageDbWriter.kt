package fi.hsl.transitdata.eke_sink

import fi.hsl.transitdata.eke_sink.EkeBinaryParser.INDEX
import fi.hsl.transitdata.eke_sink.EkeBinaryParser.SPEED
import fi.hsl.transitdata.eke_sink.EkeBinaryParser.TIMESTAMP
import fi.hsl.transitdata.eke_sink.EkeBinaryParser.TRAIN_NUMBER
import mu.KotlinLogging
import java.io.File
import java.sql.Connection
import java.sql.Date
import java.sql.Timestamp

class EkeMessageDbWriter (val dbConnection : Connection) {

    private val log = KotlinLogging.logger {}


    fun writeMessageToDb(message: ByteArray){
        val query = File("src/main/resources/save_message.sql").readText()
        val statement = dbConnection.prepareStatement(query)
        statement.setBlob(1, message.inputStream())
        statement.setTimestamp(2, Timestamp(message.readField(TIMESTAMP).time))
        statement.setInt(3, message.readField(TRAIN_NUMBER))
        statement.setInt(4, message.readField(INDEX))
        statement.setFloat(5, message.readField(SPEED))
        statement.executeUpdate()
    }
}