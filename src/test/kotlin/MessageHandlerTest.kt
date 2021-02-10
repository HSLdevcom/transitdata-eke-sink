import com.google.protobuf.ByteString
import com.nhaarman.mockitokotlin2.any
import com.nhaarman.mockitokotlin2.doReturn
import com.nhaarman.mockitokotlin2.mock
import fi.hsl.common.mqtt.proto.Mqtt
import fi.hsl.common.pulsar.PulsarApplicationContext
import fi.hsl.transitdata.eke_sink.EkeBinaryParser
import fi.hsl.transitdata.eke_sink.EkeMessageDbWriter
import fi.hsl.transitdata.eke_sink.MESSAGE_SIZE
import fi.hsl.transitdata.eke_sink.MessageHandler
import org.apache.pulsar.client.api.Consumer
import org.apache.pulsar.client.api.Message
import org.apache.pulsar.client.api.MessageId
import org.json.JSONObject
import org.junit.Assert
import org.junit.Assert.assertTrue
import org.junit.Before
import org.junit.Test
import org.mockito.Mockito
import java.io.File
import java.net.URL
import java.nio.charset.StandardCharsets
import java.util.*
import java.util.concurrent.CompletableFuture
import java.util.function.BiFunction

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
        val directory : File = File("json")
        if(!directory.exists()) directory.mkdir()
        val handler = MessageHandler(mockContext, directory)
        handler.handleMessage(createMessage())
        Mockito.verify(mockConsumer, Mockito.times(1)).acknowledgeAsync(any<MessageId>())
        val file = File(directory, "day_01-01-1970-02_vehicle_7777.json")
        assertTrue(file.exists())
        File(directory, "day_01-01-1970-02_vehicle_7777.json").inputStream().use {
            inputStream ->
            val string = Scanner(inputStream, StandardCharsets.UTF_8.name()).useDelimiter("\\A").next()
            val json = JSONObject(string)
            Assert.assertEquals(25.1f, json.getFloat(EkeBinaryParser.CATENARY_VOLTAGE.jsonFieldName))
            Assert.assertEquals(100, json.get(EkeBinaryParser.TOILET_FRESH_WATER_LEVEL.jsonFieldName))
            Assert.assertEquals(0f, json.getFloat(EkeBinaryParser.SPEED.jsonFieldName))
            Assert.assertEquals(7.4f, json.getFloat(EkeBinaryParser.OUTSIDE_TEMP.jsonFieldName))
        }
        file.delete()
        directory.delete()
    }

    private fun createMessage() : Message<Any> {
        var payload = ByteArray(MESSAGE_SIZE);
        File("src/test/resources/sm5_1_20200303_a").inputStream().use { inputStream ->
            inputStream.read(payload)
            payload = byteArrayOf(0.toByte(),0.toByte(),0.toByte(),0.toByte(),0.toByte(),0.toByte(),0.toByte(),0.toByte()) + payload
        }
        val topic = "/topic/with/json/payload/#"
        val mapper: BiFunction<String, ByteArray, ByteArray> = createMapper()
        val mapped = mapper.apply(topic, payload)
        val mqttMessage = Mqtt.RawMessage.parseFrom(mapped)
        var pulsarMessage = mock<Message<Any>> {
            on{eventTime} doReturn (Date().time)
            on{data} doReturn (mqttMessage.toByteArray())
            on{messageId} doReturn (mock())
            on{getProperty("protobuf-schema")} doReturn ("mqtt-raw")
        }
        return pulsarMessage
    }

    private fun createMapper(): BiFunction<String, ByteArray, ByteArray> {
        return BiFunction { topic: String?, payload: ByteArray? ->
            val builder = Mqtt.RawMessage.newBuilder()
            val raw = builder
                    .setSchemaVersion(builder.schemaVersion)
                    .setTopic(topic)
                    .setPayload(ByteString.copyFrom(payload))
                    .build()
            raw.toByteArray()
        }
    }
}