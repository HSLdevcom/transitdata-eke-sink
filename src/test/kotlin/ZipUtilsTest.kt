import fi.hsl.transitdata.eke_sink.gzip
import org.hamcrest.MatcherAssert.assertThat
import org.hamcrest.Matchers
import org.junit.Assert.assertTrue
import org.junit.Rule
import org.junit.Test
import org.junit.rules.TemporaryFolder
import java.nio.file.Files
import kotlin.io.path.ExperimentalPathApi
import kotlin.io.path.writeBytes
import kotlin.random.Random

class ZipUtilsTest {
    @Rule
    @JvmField
    val temporaryFolder = TemporaryFolder()

    @ExperimentalPathApi
    @Test
    fun gzipTest() {
        val uncompressed = temporaryFolder.newFile().toPath()
        val compressible = ByteArray(1024 * 1024)
        compressible.fill(7, 0, compressible.size)
        val randomData = Random.Default.nextBytes(1024 * 1024)
        uncompressed.writeBytes(compressible + randomData)

        val compressed = gzip(uncompressed)
        assertTrue(Files.exists(compressed))
        assertThat("Compressed name ends with .gz", compressed.fileName.toString(), Matchers.endsWith(".gz"))
        assertThat("Compressed size < uncompressed size", Files.size(uncompressed) - Files.size(compressed), Matchers.greaterThan(0))
    }
}