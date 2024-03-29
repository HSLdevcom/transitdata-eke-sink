import fi.hsl.transitdata.eke_sink.messages.MqttHeader
import org.junit.Assert.*
import org.junit.Test

class MqttHeaderTest {
    @Test
    fun `Test parsing MQTT header`() {
        val rawData = this.javaClass.getResourceAsStream("stadlerUDP.dat").use { it.readBytes() }

        val mqttHeader = MqttHeader.parseFromByteArray(rawData)

        assertEquals(1, mqttHeader.messageType)
        assertTrue(mqttHeader.ntpValid)
        assertEquals(1625156879, mqttHeader.ekeTime)
        assertEquals(1625146077, mqttHeader.ntpTime)
    }
}