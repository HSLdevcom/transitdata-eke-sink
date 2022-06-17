import com.google.protobuf.ByteString
import com.nhaarman.mockitokotlin2.any
import com.nhaarman.mockitokotlin2.doAnswer
import com.nhaarman.mockitokotlin2.doReturn
import com.nhaarman.mockitokotlin2.mock
import com.typesafe.config.Config
import fi.hsl.common.mqtt.proto.Mqtt
import fi.hsl.common.pulsar.PulsarApplicationContext
import fi.hsl.transitdata.eke_sink.CsvService
import fi.hsl.transitdata.eke_sink.MessageHandler
import fi.hsl.transitdata.eke_sink.sink.LocalSink
import org.apache.pulsar.client.api.*
import org.bouncycastle.asn1.iana.IANAObjectIdentifiers.directory
import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import org.junit.Before
import org.junit.Rule
import org.junit.Test
import org.junit.rules.TemporaryFolder
import org.mockito.Mockito
import java.io.File
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.nio.file.Paths
import java.time.Duration
import java.util.*
import java.util.concurrent.CompletableFuture
import kotlin.io.path.exists
import kotlin.random.Random
import kotlin.streams.toList
import kotlin.time.ExperimentalTime

@ExperimentalTime
class MessageHandlerTest {
    @Rule
    @JvmField val temp = TemporaryFolder()

    lateinit var mockContext: PulsarApplicationContext
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
        val mockConfig = mock<Config> {
            on { getBoolean(any()) } doReturn(false)
        }
        mockContext = mock<PulsarApplicationContext>{
            on { consumer } doReturn (mockConsumer)
            on { config } doReturn(mockConfig)
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

        val handler = MessageHandler(mockContext, CsvService(directory, LocalSink(uploadDirectory), mockConsumer::acknowledgeAsync, uploadAfterNotModified = Duration.ofSeconds(1), tryUploadInterval = Duration.ofSeconds(2)))
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

