import fi.hsl.transitdata.eke_sink.messages.stadlerUDP.StadlerUDPParser
import fi.hsl.transitdata.eke_sink.messages.stadlerUDP.StadlerUDPParser.CATENARY_VOLTAGE
import fi.hsl.transitdata.eke_sink.messages.stadlerUDP.StadlerUDPParser.NUMBER_OF_KILOMETERS
import fi.hsl.transitdata.eke_sink.messages.stadlerUDP.StadlerUDPParser.OUTSIDE_TEMP
import fi.hsl.transitdata.eke_sink.messages.stadlerUDP.StadlerUDPParser.SPEED
import fi.hsl.transitdata.eke_sink.messages.stadlerUDP.StadlerUDPParser.TOILET_FRESH_WATER_LEVEL


import org.json.JSONObject
import org.junit.Assert.assertEquals
import org.junit.Test

class BinaryToJsonTest {

    @Test
    fun convertTest() {
        val byteArray = getFirstRawMessage()
        val jsonString = StadlerUDPParser.toJson(byteArray, "test")
        val jsonObject = JSONObject(jsonString)
        assertEquals(1380291, jsonObject.get(NUMBER_OF_KILOMETERS.fieldName))
        /*assertEquals(60.17161458333333, jsonObject.getFloat(GPSX.jsonFieldName))
        assertEquals(24.9414794921875, jsonObject.getFloat(GPSY.jsonFieldName))*/
        assertEquals(25.1f, jsonObject.getFloat(CATENARY_VOLTAGE.fieldName))
        assertEquals(100, jsonObject.get(TOILET_FRESH_WATER_LEVEL.fieldName))
        assertEquals(0f, jsonObject.getFloat(SPEED.fieldName))
        assertEquals(7.4f, jsonObject.getFloat(OUTSIDE_TEMP.fieldName))
    }
}