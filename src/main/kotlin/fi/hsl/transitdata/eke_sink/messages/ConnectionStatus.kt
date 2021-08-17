package fi.hsl.transitdata.eke_sink.messages

import java.time.LocalDateTime
import java.time.ZoneId
import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter

data class ConnectionStatus(val status: String, val connected: Boolean, val timestamp: ZonedDateTime?) {
    companion object {
        const val CONNECTION_STATUS_MESSAGE_TYPE = 99

        private const val TIMESTAMP_FORMAT = "yyyyMMdd'T'HHmmss'Z'"

        private val TIMEZONE = ZoneId.of("Europe/Helsinki")

        private fun parseConnectionStatusTimestamp(string: String): ZonedDateTime {
            return LocalDateTime.parse(string, DateTimeFormatter.ofPattern(TIMESTAMP_FORMAT)).atZone(TIMEZONE)
        }

        fun parseConnectionStatus(byteArray: ByteArray): ConnectionStatus? {
            val messageString = byteArray.toString(Charsets.UTF_8)

            return when {
                messageString.startsWith("disconnected") -> {
                    //Disconnected last-will message, no timestamp
                    ConnectionStatus(messageString, false, null)
                }
                messageString.startsWith("Disconnected at") -> {
                    ConnectionStatus(messageString, false, parseConnectionStatusTimestamp(messageString.split(" ").last()))
                }
                messageString.startsWith("Connected at") -> {
                    ConnectionStatus(messageString, true, parseConnectionStatusTimestamp(messageString.split(" ").last()))
                }
                else -> {
                    null
                }
            }
        }
    }
}
