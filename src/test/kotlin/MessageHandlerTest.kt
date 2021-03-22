import com.nhaarman.mockitokotlin2.any
import com.nhaarman.mockitokotlin2.doReturn
import com.nhaarman.mockitokotlin2.mock
import fi.hsl.common.pulsar.PulsarApplicationContext
import fi.hsl.transitdata.eke_sink.messages.stadlerUDP.EkeMessageDbWriter
import fi.hsl.transitdata.eke_sink.MessageHandler
import org.apache.pulsar.client.api.Consumer
import org.apache.pulsar.client.api.MessageId
import org.junit.Assert.assertTrue
import org.junit.Before
import org.junit.Test
import org.mockito.Mockito
import java.io.File
import java.nio.charset.StandardCharsets
import java.util.*
import java.util.concurrent.CompletableFuture

class MessageHandlerTest {

    lateinit var mockContext: PulsarApplicationContext
    lateinit var mockConsumer: Consumer<ByteArray>
    lateinit var mockDbWriter : EkeMessageDbWriter

    @Before
    fun before(){
        mockConsumer = mock<Consumer<ByteArray>>{
            on{acknowledgeAsync(any<MessageId>())} doReturn (CompletableFuture<Void>())
        }
        mockContext = mock<PulsarApplicationContext>{
            on{consumer} doReturn (mockConsumer)
        }
        mockDbWriter = mock {
        }
    }

    @Test
    fun handleMessageTest(){
        val directory : File = File("eke")
        if(!directory.exists()) directory.mkdir()
        val handler = MessageHandler(mockContext, directory, "csv")
        handler.handleMessage(createFirstMqttMessage())
        handler.ackMessages()
        Mockito.verify(mockConsumer, Mockito.times(1)).acknowledgeAsync(any<MessageId>())
        val file = File(directory, "day_01-01-1970-02_vehicle_7777.csv")
        assertTrue(file.exists())
        File(directory, "day_01-01-1970-02_vehicle_7777.csv").inputStream().use {
            inputStream ->
            val string = Scanner(inputStream, StandardCharsets.UTF_8.name()).useDelimiter("\\A").next()
            //Do some test
        }
        file.delete()
        directory.delete()
    }

    @Test
    fun handleMessagesTest(){
        val directory : File = File("eke")
        if(!directory.exists()) directory.mkdir()
        val handler = MessageHandler(mockContext, directory, "csv")
        getInputStreamFromTestFile().use { inputStream ->
            while(inputStream.available() > 0){
                val message = createMqttMessage(inputStream)
                handler.handleMessage(message)
            }
        }
        handler.ackMessages()
        Mockito.verify(mockConsumer, Mockito.times(1)).acknowledgeAsync(any<MessageId>())

        val file = File(directory, "day_01-01-1970-02_vehicle_7777.csv")
        assertTrue(file.exists())
        file.delete()
        directory.delete()
    }
}