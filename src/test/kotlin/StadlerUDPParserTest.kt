import fi.hsl.transitdata.eke_sink.*
import fi.hsl.transitdata.eke_sink.converters.readField
import fi.hsl.transitdata.eke_sink.messages.stadlerUDP.*
import org.junit.Assert.assertEquals
import org.junit.Test
import java.nio.charset.StandardCharsets
import kotlin.experimental.and

class StadlerUDPParserTest {


    private val hexArray = "0123456789ABCDEF".toCharArray()

    private val HEX_ARRAY: ByteArray = "0123456789ABCDEF".toByteArray(StandardCharsets.US_ASCII)

    //For debug
    fun bytesToHex(bytes: ByteArray): String? {
        val hexChars = ByteArray(bytes.size * 2)
        for (j in bytes.indices) {
            val v = (bytes[j] and 0xFF.toByte()).toInt()
            hexChars[j * 2] = HEX_ARRAY.get(v ushr 4)
            hexChars[j * 2 + 1] = HEX_ARRAY.get(v and 0x0F)
        }
        return String(hexChars, StandardCharsets.UTF_8)
    }

    /**
     * Test the content of the first message of the file
     */
    @Test
    fun testParseOneLine(){
        val byteArray = getFirstRawMessage()
        assertEquals(74, byteArray.readField(INDEX))
        assertEquals(0, byteArray.readField(VEHICLE_SHUTTING_DOWN))
        assertEquals(0.0f, byteArray.readField(SPEED))
        assertEquals(11952, byteArray.readField(ODOMETER))
        assertEquals(1380291, byteArray.readField(NUMBER_OF_KILOMETERS))
        assertEquals(0.0f, byteArray.readField(ACCELERATION))
        assertEquals(1, byteArray.readField(STANDSTILL))
        assertEquals(13809937, byteArray.readField(ENERGY_CONSUMPTION))
        assertEquals(5013116, byteArray.readField(ENERGY_RECUPERATION))
        assertEquals(251, byteArray.readField(CATENARY_VOLTAGE))
        assertEquals(19, byteArray.readField(INSIDE_TEMP_COACH_A))
        assertEquals(74, byteArray.readField(OUTSIDE_TEMP))
        assertEquals(1, byteArray.readField(NUMBER_OF_VEHICLES))
        assertEquals(7777, byteArray.readField(TRAIN_NUMBER))
        //assertEquals(60.171616f, byteArray.readField(GPSX))
        //assertEquals(24.941479f, byteArray.readField(GPSY))
    }
}