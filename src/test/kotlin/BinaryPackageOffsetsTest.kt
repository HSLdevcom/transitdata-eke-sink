import fi.hsl.transitdata.eke_sink.*
import org.junit.Assert.assertEquals
import org.junit.Test

class BinaryPackageOffsetsTest {

    @Test
    fun testOffsets(){
        assertEquals(36, TRACTION_LEVER_POSITION_OFFSET)
        assertEquals(80, POWER_CONVERTER_1A_COOLING_WATER_TEMP_OFFSET)
        assertEquals(144, FLAGS_OFFSET)
        assertEquals(172, TIME_OFFSET)

        val toto = ByteArray(10)
        val byte = toto[INDEX_OFFSET]
        val int = byte.toInt()

        byte.toFloat()
    }
}