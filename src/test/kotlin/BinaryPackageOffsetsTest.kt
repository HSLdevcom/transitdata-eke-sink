import fi.hsl.transitdata.eke_sink.*
import org.junit.Assert.assertEquals
import org.junit.Test

class BinaryPackageOffsetsTest {

    @Test
    fun testOffsets(){
        assertEquals(44, TRACTION_LEVER_POSITION_OFFSET)
        assertEquals(86, POWER_CONVERTER_1A_COOLING_WATER_TEMP_OFFSET)
        assertEquals(152, FLAGS_OFFSET)
        assertEquals(180, TIME_OFFSET)
    }
}