package fi.hsl.transitdata.eke_sink

import fi.hsl.transitdata.eke_sink.EkeBinaryParser.CATENARY_VOLTAGE
import fi.hsl.transitdata.eke_sink.EkeBinaryParser.EKE_TIME
import fi.hsl.transitdata.eke_sink.EkeBinaryParser.INDEX
import fi.hsl.transitdata.eke_sink.EkeBinaryParser.SPEED
import fi.hsl.transitdata.eke_sink.EkeBinaryParser.TRAIN_NUMBER
import mu.KotlinLogging
import java.io.File
import java.sql.Connection
import java.sql.Date
import java.sql.Timestamp

class EkeMessageDbWriter (val dbConnection : Connection) {

    private val log = KotlinLogging.logger {}
    val query = File("src/main/resources/save_message.sql").readText()


    fun writeMessageToDb(message: ByteArray){
        val statement = dbConnection.prepareStatement(query)
        val inputStream = message.inputStream()
        statement.setBinaryStream(1, inputStream, inputStream.available())
        statement.setTimestamp(2, Timestamp(message.readField(EKE_TIME).time))
        statement.setInt(3, message.readField(TRAIN_NUMBER))
        statement.setInt(4, message.readField(INDEX))
        statement.setFloat(5, message.readField(SPEED))
        statement.setInt(6, message.readField(CATENARY_VOLTAGE))
        statement.executeUpdate()
        dbConnection.commit()
    }
}