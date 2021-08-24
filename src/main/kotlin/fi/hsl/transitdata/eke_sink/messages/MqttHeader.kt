package fi.hsl.transitdata.eke_sink.messages

import java.time.*
import kotlin.math.max
import kotlin.math.min

/**
 * Describes MQTT header used in EKE messages
 *
 * @property ekeTime Timestamp (in seconds) in local time
 * @property ntpTime Timestamp (in seconds) in UTC
 */
data class MqttHeader(
    val messageType: Int,
    val version: Int,
    val ntpValid: Boolean,
    val ekeTime: Int,
    val ekeTimeHundredsOfSecond: Int,
    val ntpTime: Int,
    val ntpTimeHundredsOfSecond: Int) {

    companion object {
        fun parseFromByteArray(byteArray: ByteArray): MqttHeader {
            /**
             * 5 bit message type | 10 bit version | 1 bit ntp timestamp valid = total 16 bit
             * 32bit + 8 bit capture timestamp (eke time) unixtime + hundreds of second
             * 32bit + 8 bit capture timestamp corrected with ntp time difference (ntp time) unixtime + hundreds of second
             * (header total length 12 bytes)
             */
            if (byteArray.size < 12) {
                throw IllegalArgumentException("Byte array that contains the header must be at least 12 bytes long")
            }

            val header = byteArray.copyOf(12)

            val firstPart = bytesToInt(fixSize(header.copyOfRange(0, 2)))
            val messageType = firstPart and 0x1f
            val version = (firstPart shr 5) and 0x3ff
            val ntpValid = (firstPart shr 15) == 1

            val ekeTime = bytesToInt(fixSize(header.copyOfRange(2, 6)))
            val ekeTimeHundredsOfSecond = bytesToInt((fixSize(header.copyOfRange(6, 7))))

            val ntpTime = bytesToInt(fixSize(header.copyOfRange(7, 11)))
            val ntpTimeHundredsOfSecond = bytesToInt((fixSize(header.copyOfRange(11, 12))))

            return MqttHeader(messageType, version, ntpValid, ekeTime, ekeTimeHundredsOfSecond, ntpTime, ntpTimeHundredsOfSecond)
        }

        private fun bytesToInt(bytes: ByteArray): Int {
            return (bytes[0].toInt() shl 24) or (bytes[1].toInt() and 0xFF shl 16) or (bytes[2].toInt() and 0xFF shl 8) or (bytes[3].toInt() and 0xFF)
        }

        /**
         * Changes byte array size to (at least) 4 bytes so that it can be parsed as int
         */
        private fun fixSize(byteArray: ByteArray): ByteArray {
            val size = max(0, min(4,4 - byteArray.size))
            val zeroFilled = ByteArray(size)
            zeroFilled.fill(0)

            return zeroFilled + byteArray
        }
    }

    /**
     * EKE time, calculated from ekeTime and ekeTimeHundredsOfSecond
     */
    val ekeTimeExact: ZonedDateTime = Instant.ofEpochSecond(ekeTime.toLong())
        .plusMillis(ekeTimeHundredsOfSecond * 10L)
        .atZone(ZoneId.of("UTC"))
        .withZoneSameLocal(ZoneId.of("Europe/Helsinki"))

    /**
     * NTP time, calculated from ntpTime and ntpTimeHundredsOfSecond
     */
    val ntpTimeExact: ZonedDateTime = Instant.ofEpochSecond(ntpTime.toLong())
        .plusMillis(ntpTimeHundredsOfSecond * 10L)
        .atZone(ZoneId.of("Europe/Helsinki"))
        .withZoneSameInstant(ZoneId.of("UTC"))
}