import com.google.protobuf.ByteString
import fi.hsl.common.mqtt.proto.Mqtt
import fi.hsl.transitdata.eke_sink.CsvService
import fi.hsl.transitdata.eke_sink.MessageHandler
import fi.hsl.transitdata.eke_sink.sink.LocalSink
import org.apache.pulsar.client.api.Consumer
import org.apache.pulsar.client.api.Message
import org.apache.pulsar.client.api.MessageId
import org.apache.pulsar.client.api.TypedMessageBuilder
import org.junit.Assert.assertEquals
import org.junit.Before
import org.junit.Rule
import org.junit.Test
import org.junit.rules.TemporaryFolder
import org.mockito.Mockito
import org.mockito.kotlin.any
import org.mockito.kotlin.doAnswer
import org.mockito.kotlin.doReturn
import org.mockito.kotlin.mock
import java.nio.file.Files
import java.time.Duration
import java.util.*
import java.util.concurrent.CompletableFuture
import kotlin.streams.toList
import kotlin.time.ExperimentalTime

@ExperimentalTime
class MessageHandlerTest {
    @Rule
    @JvmField val temp = TemporaryFolder()

    lateinit var mockConsumer: Consumer<ByteArray>
    lateinit var mockMessage : TypedMessageBuilder<ByteArray>

    @Before
    fun before() {
        mockConsumer = mock<Consumer<ByteArray>>{
            on { acknowledgeAsync(any<MessageId>()) } doReturn (CompletableFuture<Void>())
            on { acknowledgeCumulativeAsync(any<MessageId>()) } doReturn(CompletableFuture())
        }
        mockMessage = mock<TypedMessageBuilder<ByteArray>>(stubOnly = true){
            on { sendAsync() } doReturn (CompletableFuture<MessageId>())
            on { property(any<String>(), any<String>()) } doReturn (it)
            on { value(any<ByteArray>()) } doReturn(it)
        }
    }

    @Test
    fun handleMessageTest() {
        val directory = temp.newFolder("eke").toPath()
        if (Files.notExists(directory)) {
            Files.createDirectories(directory)
        }

        val uploadDirectory = temp.newFolder("blobstorage").toPath()
        if (Files.notExists(uploadDirectory)) {
            Files.createDirectories(uploadDirectory)
        }

        val handler = MessageHandler(CsvService(directory, LocalSink(uploadDirectory), mockConsumer::acknowledgeAsync, uploadAfterNotModified = Duration.ofSeconds(1), tryUploadInterval = Duration.ofSeconds(2)))
        handler.handleMessage(mock<Message<Any>> {
            on { data } doAnswer { Mqtt.RawMessage.newBuilder().setPayload(ByteString.copyFrom(getMessageContent())).setTopic("eke/v1/sm5/15/A/stadlerUDP").setSchemaVersion(1).build().toByteArray() }
            on { properties } doReturn(Collections.singletonMap(fi.hsl.common.transitdata.TransitdataProperties.KEY_SOURCE_MESSAGE_TIMESTAMP_MS, java.time.Instant.now().toEpochMilli().toString()))
            on { messageId } doReturn(mock<MessageId>())
        })

        Thread.sleep(5000)

        val directoryContent = Files.list(uploadDirectory).toList()
        //There should be one file
        assertEquals(1, directoryContent.size)

        Mockito.verify(mockConsumer, Mockito.times(1)).acknowledgeAsync(any<MessageId>())
    }

    private fun getMessageContent(): ByteArray = this::class.java.getResourceAsStream("stadlerUDP.dat").use { it.readBytes() }
}

