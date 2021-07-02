import fi.hsl.transitdata.eke_sink.CSVHelper
import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import org.junit.Before
import org.junit.Rule
import org.junit.Test
import org.junit.rules.TemporaryFolder
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.nio.file.Path
import java.time.Duration
import kotlin.io.path.ExperimentalPathApi
import kotlin.io.path.readLines

class CSVHelperTest {
    @Rule
    @JvmField
    val temporaryFolder = TemporaryFolder()

    private lateinit var directory: Path
    private lateinit var csvHelper: CSVHelper

    @Before
    fun setup() {
        directory = temporaryFolder.newFolder().toPath()
        csvHelper = CSVHelper(directory, Duration.ofSeconds(2), listOf("a", "b"))
    }

    @ExperimentalPathApi
    @Test
    fun `Test writing CSV`() {
        csvHelper.writeToCsv("test", listOf("1", "2"))
        csvHelper.writeToCsv("test", listOf("3", "4"))

        Thread.sleep(3000)

        val csvFile = directory.resolve("test.csv")
        assertTrue(Files.exists(csvFile))

        val content = csvFile.readLines(StandardCharsets.UTF_8)
        assertEquals(3, content.size)
        assertEquals("a,b", content[0])
        assertEquals("1,2", content[1])
        assertEquals("3,4", content[2])
    }
}