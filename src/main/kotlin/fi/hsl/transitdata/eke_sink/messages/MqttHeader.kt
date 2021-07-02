package fi.hsl.transitdata.eke_sink.messages

import java.time.*
import java.util.*
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

            val header = BitSet.valueOf(byteArray.copyOf(12))

            val messageType = bytesToInt(fixSize(header.get(0, 5).toByteArray()))
            val version = bytesToInt(fixSize(header.get(5, 15).toByteArray()))
            val ntpValid = header.get(15)

            val ekeTime = bytesToInt((header.get(16, 48).toByteArray()))
            val ekeTimeHundredsOfSecond = bytesToInt((fixSize(header.get(48, 56).toByteArray())))

            val ntpTime = bytesToInt((header.get(56, 88).toByteArray()))
            val ntpTimeHundredsOfSecond = bytesToInt((fixSize(header.get(88, 96).toByteArray())))

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