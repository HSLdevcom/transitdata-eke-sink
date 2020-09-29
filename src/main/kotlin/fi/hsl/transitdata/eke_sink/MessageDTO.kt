package fi.hsl.transitdata.eke_sink

import java.util.*

data class MessageDTO(
    val rawData : ByteArray,
    val index : Int,
    val date : Date,
    val trainId : Int,
    val speed : Float
) {
    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as MessageDTO

        if (!rawData.contentEquals(other.rawData)) return false
        if (date != other.date) return false
        if (trainId != other.trainId) return false
        if (speed != other.speed) return false

        return true
    }

    override fun hashCode(): Int {
        var result = rawData.contentHashCode()
        result = 31 * result + date.hashCode()
        result = 31 * result + trainId
        result = 31 * result + speed.hashCode()
        return result
    }
}