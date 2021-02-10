import com.nhaarman.mockitokotlin2.any
import com.nhaarman.mockitokotlin2.doReturn
import com.nhaarman.mockitokotlin2.mock
import fi.hsl.common.pulsar.PulsarApplicationContext
import fi.hsl.transitdata.eke_sink.*
import fi.hsl.transitdata.eke_sink.EkeBinaryParser.CATENARY_VOLTAGE
import fi.hsl.transitdata.eke_sink.EkeBinaryParser.GPSX
import fi.hsl.transitdata.eke_sink.EkeBinaryParser.GPSY
import fi.hsl.transitdata.eke_sink.EkeBinaryParser.MQTT_HEADER_1ST_PART
import fi.hsl.transitdata.eke_sink.EkeBinaryParser.NUMBER_OF_KILOMETERS
import fi.hsl.transitdata.eke_sink.EkeBinaryParser.OUTSIDE_TEMP
import fi.hsl.transitdata.eke_sink.EkeBinaryParser.SPEED
import fi.hsl.transitdata.eke_sink.EkeBinaryParser.TOILET_FRESH_WATER_LEVEL
import org.apache.pulsar.client.api.Consumer
import org.apache.pulsar.client.api.MessageId
import org.json.JSONObject
import org.junit.Assert.assertEquals
import org.junit.Before
import org.junit.Test
import java.io.File
import java.math.BigDecimal
import java.util.concurrent.CompletableFuture

class BinaryToJsonTest {

    @Test
    fun convertTest(){
        var byteArray = ByteArray(MESSAGE_SIZE);
        val outputPath = File("json")
        File("src/test/resources/sm5_1_20200303_a").inputStream().use { inputStream ->
            inputStream.read(byteArray)
            //Test file is missing the first bytes, let's add some padding
            byteArray = byteArrayOf(0.toByte(),0.toByte(),0.toByte(),0.toByte(),0.toByte(),0.toByte(),0.toByte(),0.toByte()) + byteArray
            val jsonString = EkeBinaryParser.toJson(byteArray)
            val jsonObject = JSONObject(jsonString)
            assertEquals(1380291, jsonObject.get(NUMBER_OF_KILOMETERS.jsonFieldName))
            /*assertEquals(60.17161458333333, jsonObject.getFloat(GPSX.jsonFieldName))
            assertEquals(24.9414794921875, jsonObject.getFloat(GPSY.jsonFieldName))*/
            assertEquals(25.1f, jsonObject.getFloat(CATENARY_VOLTAGE.jsonFieldName))
            assertEquals(100, jsonObject.get(TOILET_FRESH_WATER_LEVEL.jsonFieldName))
            assertEquals(0f, jsonObject.getFloat(SPEED.jsonFieldName))
            assertEquals(7.4f, jsonObject.getFloat(OUTSIDE_TEMP.jsonFieldName))
        }
    }

}