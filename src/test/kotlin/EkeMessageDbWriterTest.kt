import fi.hsl.transitdata.eke_sink.EkeBinaryParser.INDEX
import fi.hsl.transitdata.eke_sink.EkeBinaryParser.SPEED
import fi.hsl.transitdata.eke_sink.EkeBinaryParser.TIMESTAMP
import fi.hsl.transitdata.eke_sink.EkeBinaryParser.TRAIN_NUMBER
import fi.hsl.transitdata.eke_sink.EkeMessageDbWriter
import fi.hsl.transitdata.eke_sink.MESSAGE_SIZE
import fi.hsl.transitdata.eke_sink.readField
import liquibase.Contexts
import liquibase.LabelExpression
import liquibase.Liquibase
import liquibase.database.DatabaseFactory
import liquibase.database.jvm.JdbcConnection
import liquibase.resource.ClassLoaderResourceAccessor
import org.junit.Assert.assertEquals
import org.junit.BeforeClass
import org.junit.Test
import java.io.File
import java.sql.Connection
import java.sql.DriverManager
import java.sql.ResultSet
import java.util.*

class EkeMessageDbWriterTest {

    @Test
    fun testWriteOneMessageToDb(){
        val byteArray = ByteArray(MESSAGE_SIZE);

        DriverManager.getConnection("jdbc:h2:mem:test1", "", "" ).use { connection ->
            val database = DatabaseFactory.getInstance().findCorrectDatabaseImplementation(JdbcConnection(connection))
            val liquibase = Liquibase("migrations.xml", ClassLoaderResourceAccessor(), database)
            liquibase.update(Contexts(), LabelExpression())
            File("src/test/resources/sm5_1_20200303_a").inputStream().use { inputStream ->
                inputStream.read(byteArray)
                val writer = EkeMessageDbWriter(connection)
                writer.writeMessageToDb(byteArray)
                val resultSet = connection.prepareStatement("select * from message").executeQuery()
                //assertEquals(1, resultSet.fetchSize)
                resultSet.next()
                assertEquals(byteArray.size, resultSet.getBlob(1).length().toInt())
                assertEquals(byteArray.readField(TIMESTAMP), resultSet.getTimestamp(2))
                assertEquals(byteArray.readField(TRAIN_NUMBER), resultSet.getInt(3))
                assertEquals(byteArray.readField(INDEX), resultSet.getInt(4))
                assertEquals(byteArray.readField(SPEED), resultSet.getFloat(5))
            }
        }
    }

    @Test
    fun testWriteAllMessagesToDb(){
        val byteArray = ByteArray(MESSAGE_SIZE);

        DriverManager.getConnection("jdbc:h2:mem:test2", "", "" ).use { connection ->
            val database = DatabaseFactory.getInstance().findCorrectDatabaseImplementation(JdbcConnection(connection))
            val liquibase = Liquibase("migrations.xml", ClassLoaderResourceAccessor(), database)
            liquibase.update(Contexts(), LabelExpression())
            File("src/test/resources/sm5_1_20200303_a").inputStream().use { inputStream ->
                val writer = EkeMessageDbWriter(connection)
                while(inputStream.read(byteArray) != -1){
                    writer.writeMessageToDb(byteArray)
                }
                val resultSet = connection.prepareStatement("select count(1) from message").executeQuery()
                resultSet.next()
                assertEquals(172836, resultSet.getLong(1))
            }
        }
    }
}