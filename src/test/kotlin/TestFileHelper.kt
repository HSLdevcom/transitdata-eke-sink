import com.google.protobuf.ByteString
import com.nhaarman.mockitokotlin2.doReturn
import com.nhaarman.mockitokotlin2.mock
import fi.hsl.common.mqtt.proto.Mqtt
import fi.hsl.transitdata.eke_sink.messages.stadlerUDP.STADLER_UDP_SIZE
import org.apache.pulsar.client.api.Message
import java.io.File
import java.io.InputStream
import java.util.*
import java.util.function.BiFunction

val TIMESTAMP_SIZE = 4
val MQTT_HEADER_SIZE = 12

fun getInputStreamFromTestFile() : InputStream {
    return File("src/test/resources/sm5_1_20200303_a").inputStream()
}

fun getFirstRawMessage() : ByteArray{
    getInputStreamFromTestFile().use {
        return getRawMessage(it)
    }
}

fun getRawMessage (inputStream : InputStream) : ByteArray{
    val arraySize = STADLER_UDP_SIZE - MQTT_HEADER_SIZE + TIMESTAMP_SIZE
    var payload = ByteArray(arraySize)
    inputStream.read(payload)
    //The first bytes are a timestamp, missing from mqtt data, let's remove it
    payload = payload.copyOfRange(4, arraySize)
    //Test file is missing the 12 first bytes, let's add some padding
    return byteArrayOf(0.toByte(),0.toByte(),0.toByte(),0.toByte(),0.toByte(),0.toByte(),0.toByte(),0.toByte(),0.toByte(),0.toByte(),0.toByte(),0.toByte()) + payload
}

fun createMqttMessage(inputStream: InputStream, topic: String): Message<Any> {
    val payload = getRawMessage(inputStream)
    val topic = topic
    val mapper: BiFunction<String, ByteArray, ByteArray> = createMapper()
    val mapped = mapper.apply(topic, payload)
    val mqttMessage = Mqtt.RawMessage.parseFrom(mapped)
    return mock {
        on { eventTime } doReturn (Date().time)
        on { data } doReturn (mqttMessage.toByteArray())
        on { messageId } doReturn (mock())
        on { getProperty("protobuf-schema") } doReturn ("mqtt-raw")
    }
}

fun createFirstMqttMessage() : Message<Any> {
    var payload = ByteArray(STADLER_UDP_SIZE)
    File("src/test/resources/sm5_1_20200303_a").inputStream().use { inputStream ->
        return createMqttMessage(inputStream, "eke/v1/sm5/7777/stadlerUDP")
    }

}

fun createMapper(): BiFunction<String, ByteArray, ByteArray> {
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