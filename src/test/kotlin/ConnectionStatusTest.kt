import fi.hsl.transitdata.eke_sink.messages.ConnectionStatus
import org.junit.Assert.*
import org.junit.Test
import java.time.LocalDateTime

class ConnectionStatusTest {
    @Test
    fun `Test parse Connected at message`() {
        val payload = "Connected at 20210817T160000Z\u0000"

        val connectionStatus = ConnectionStatus.parseConnectionStatus(payload.toByteArray(Charsets.UTF_8))

        assertNotNull(connectionStatus)
        assertTrue(connectionStatus!!.connected)
        assertNotNull(connectionStatus.timestamp)
        assertEquals(LocalDateTime.of(2021, 8, 17, 16, 0, 0), connectionStatus.timestamp!!.toLocalDateTime())
    }
}
