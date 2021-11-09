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
import kotlin.random.Random

class CSVHelperTest {
    @Rule
    @JvmField
    val temporaryFolder = TemporaryFolder()

    private lateinit var directory: Path
    private lateinit var csvHelper: CSVHelper

    private lateinit var readyToUpload: MutableSet<Path>

    @Before
    fun setup() {
        readyToUpload = mutableSetOf()

        directory = temporaryFolder.newFolder().toPath()
        csvHelper = CSVHelper(directory, Duration.ofSeconds(5), false, listOf("a", "b"), readyToUpload::add)
    }

    @ExperimentalPathApi
    @Test
    fun `Test writing CSV`() {
        csvHelper.writeToCsv("test", listOf("1", "2"))
        csvHelper.writeToCsv("test", listOf("3", "4"))

        Thread.sleep(10000)

        val csvFile = directory.resolve("test.csv")
        assertTrue(Files.exists(csvFile))

        val content = csvFile.readLines(StandardCharsets.UTF_8)
        assertEquals(3, content.size)
        assertEquals("a,b", content[0])
        assertEquals("1,2", content[1])
        assertEquals("3,4", content[2])
    }

    @Test
    fun `Test file is added to upload list after closing`() {
        csvHelper.writeToCsv("test", listOf("1", "2"))

        Thread.sleep(10000)

        assertEquals(1, readyToUpload.size)
        assertTrue(readyToUpload.contains(directory.resolve("test.csv")))
    }

    @Test
    fun `Test with large amount of files`() {
        for (i in 1..1000) {
            csvHelper.writeToCsv("test_$i", listOf(Random.Default.nextInt().toString(), Random.Default.nextInt().toString()))
        }

        Thread.sleep(60 * 1000)

        assertEquals(1000, readyToUpload.size)
    }
}