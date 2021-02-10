package fi.hsl.transitdata.eke_sink

import java.nio.ByteBuffer
import java.util.*

interface ByteConverter<T>{
    fun toValue(bytes : ByteArray) : T
}

val TO_UNSIGNED_INT = object : ByteConverter<Int> {
    override fun toValue(bytes: ByteArray): Int {
        var tmpArray = bytes
        while(tmpArray.size < 4) tmpArray += byteArrayOf(0.toByte())
        return ByteBuffer.wrap(tmpArray.toUByteArray().asByteArray()).order(java.nio.ByteOrder.LITTLE_ENDIAN).int
    }
}

val TO_INT = object : ByteConverter<Int> {
    override fun toValue(bytes: ByteArray): Int {
        var tmpArray = bytes
        while(tmpArray.size < 4) tmpArray += byteArrayOf(0.toByte())
        return ByteBuffer.wrap(tmpArray).order(java.nio.ByteOrder.LITTLE_ENDIAN).int
    }
}

val BIG_ENDIAN_TO_INT = object : ByteConverter<Int> {
    override fun toValue(bytes: ByteArray): Int {
        var tmpArray = bytes
        while(tmpArray.size < 4) tmpArray = byteArrayOf(0.toByte()) + tmpArray
        return ByteBuffer.wrap(tmpArray).order(java.nio.ByteOrder.BIG_ENDIAN).int
    }
}

//For testing purposes
val TO_BYTE_ARRAY = object : ByteConverter<ByteArray>{
    override fun toValue(bytes: ByteArray): ByteArray {
        return bytes
    }
}

val TO_FLOAT = object : ByteConverter<Float> {
    override fun toValue(bytes: ByteArray): Float {
        return ByteBuffer.wrap(bytes).order(java.nio.ByteOrder.LITTLE_ENDIAN).float
    }
}
val TO_DATE = object : ByteConverter<Date> {
    override fun toValue(bytes: ByteArray): Date {
        return Date(ByteBuffer.wrap(bytes.toUByteArray().asByteArray()).order(java.nio.ByteOrder.LITTLE_ENDIAN).int * 1000L )
    }
}

val BIGENDIAN_TO_DATE = object : ByteConverter<Date> {
    override fun toValue(bytes: ByteArray): Date {
        return Date(ByteBuffer.wrap(bytes.toUByteArray().asByteArray()).order(java.nio.ByteOrder.BIG_ENDIAN).int * 1000L )
    }
}

val TO_UNSIGNED_INT_ARRAY = object : ByteConverter<IntArray>{
    override fun toValue(bytes: ByteArray): IntArray {
        val array = IntArray(bytes.size)
        for( i in bytes.indices){
            array[i] = TO_UNSIGNED_INT.toValue(bytes.copyOfRange(i,i + 1))
        }
        return array
    }
}

fun <T> ByteArray.readField(field : FieldDefinition<T>) : T{
    return field.readField(this)
}

class FieldDefinition<T>(val offset : Int, val size : Int, val converter : ByteConverter<T>, val jsonFieldName : String){

    fun readField(rawData : ByteArray) : T{
        return converter.toValue(rawData.copyOfRange(offset, offset + size))
    }
}