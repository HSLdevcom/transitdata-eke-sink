import fi.hsl.transitdata.eke_sink.csv.CsvFile
import org.junit.Assert.assertEquals
import org.junit.Test

class CsvFileTest {
    @Test
    fun `sanitizeTags replaces invalid char in value and keeps valid key`() {
        val inputTags = mapOf("unit_number" to "70'B")
        val expectedTags = mapOf("unit_number" to "70_B")
        val outputTags = CsvFile.sanitizeTags(inputTags)
        assertEquals("The sanitized tags do not match the expected output.", expectedTags, outputTags)
    }

    @Test
    fun `sanitizeTags returns empty map for empty input`() {
        val inputTags = emptyMap<String, String>()
        val expectedTags = emptyMap<String, String>()
        val outputTags = CsvFile.sanitizeTags(inputTags)
        assertEquals(expectedTags, outputTags)
    }
}
