import com.nhaarman.mockitokotlin2.any
import com.nhaarman.mockitokotlin2.doReturn
import com.nhaarman.mockitokotlin2.mock
import fi.hsl.common.pulsar.PulsarApplicationContext
import fi.hsl.transitdata.eke_sink.MessageHandler
import org.apache.pulsar.client.api.Consumer
import org.apache.pulsar.client.api.MessageId
import org.apache.pulsar.client.api.Producer
import org.apache.pulsar.client.api.TypedMessageBuilder
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
    lateinit var mockProducer: Producer<ByteArray>
    lateinit var mockMessage : TypedMessageBuilder<ByteArray>

    @Before
    fun before(){
        mockConsumer = mock<Consumer<ByteArray>>{
            on{ acknowledgeAsync(any<MessageId>()) } doReturn (CompletableFuture<Void>())
            on { acknowledgeCumulativeAsync(any<MessageId>()) } doReturn(CompletableFuture())
        }
        mockMessage = mock<TypedMessageBuilder<ByteArray>>(stubOnly = true){
            on{ sendAsync() } doReturn (CompletableFuture<MessageId>())
            on{ property(any<String>(), any<String>()) } doReturn (it)
            on{ value(any<ByteArray>()) } doReturn(it)
        }
        mockProducer = mock<Producer<ByteArray>>{
            on{ newMessage() } doReturn(mockMessage)
        }
        mockContext = mock<PulsarApplicationContext>{
            on{ consumer } doReturn (mockConsumer)
            on{ singleProducer } doReturn (mockProducer)
        }
    }

    @Test
    fun handleMessageTest(){
        val directory = File("eke")
        if(!directory.exists()) directory.mkdir()

        val handler = MessageHandler(mockContext, directory.toPath())
        handler.handleMessage(createFirstMqttMessage())
        handler.ackMessages()
        Mockito.verify(mockConsumer, Mockito.times(1)).acknowledgeCumulativeAsync(any<MessageId>())
        val file = File(directory, "1970-01-01T00_stadlerUDP_vehicle_7777.csv")
        assertTrue(file.exists())
        File(directory, "1970-01-01T00_stadlerUDP_vehicle_7777.csv").inputStream().use {
            inputStream ->
            val string = Scanner(inputStream, StandardCharsets.UTF_8.name()).useDelimiter("\\A").next()
            //Do some test
        }
        file.delete()
        directory.delete()
    }

    @Test
    fun handleMessagesTest(){
        val directory = File("eke")
        if(!directory.exists()) directory.mkdir()

        val handler = MessageHandler(mockContext, directory.toPath())
        getInputStreamFromTestFile().use { inputStream ->
            while(inputStream.available() > 0){
                val message = createMqttMessage(inputStream, "eke/v1/sm5/7777/stadlerUDP")
                handler.handleMessage(message)
            }
        }
        handler.ackMessages()
        Mockito.verify(mockConsumer, Mockito.times(1)).acknowledgeCumulativeAsync(any<MessageId>())

        val file = File(directory, "1970-01-01T00_stadlerUDP_vehicle_7777.csv")
        assertTrue(file.exists())
        file.delete()
        directory.delete()
    }
}

