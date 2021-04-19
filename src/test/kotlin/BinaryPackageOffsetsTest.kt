import fi.hsl.transitdata.eke_sink.*
import fi.hsl.transitdata.eke_sink.messages.stadlerUDP.FLAGS
import fi.hsl.transitdata.eke_sink.messages.stadlerUDP.POWER_CONVERTER_1A_COOLING_WATER_TEMP
import fi.hsl.transitdata.eke_sink.messages.stadlerUDP.TIME
import fi.hsl.transitdata.eke_sink.messages.stadlerUDP.TRACTION_LEVER_POSITION
import org.junit.Assert.assertEquals
import org.junit.Test

class BinaryPackageOffsetsTest {

    @Test
    fun testStadlerOffsets(){
        assertEquals(44, TRACTION_LEVER_POSITION.offset)
        assertEquals(88, POWER_CONVERTER_1A_COOLING_WATER_TEMP.offset)
        assertEquals(152, FLAGS.offset)
        assertEquals(180, TIME.offset)
    }
}