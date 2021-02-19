import fi.hsl.transitdata.eke_sink.zipFiles
import org.junit.Assert.assertTrue
import org.junit.Test
import java.io.File

class ZipUtilsTest {

    @Test
    fun zipTest(){
        val path = File("src/test/resources")
        zipFiles(path, "test.zip",path.list()!!.asList())
        assertTrue(File(path, "test.zip").exists())
        File(path, "test.zip").delete()
    }
}