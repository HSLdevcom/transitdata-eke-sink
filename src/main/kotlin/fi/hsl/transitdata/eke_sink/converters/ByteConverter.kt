package fi.hsl.transitdata.eke_sink.converters

import java.nio.ByteBuffer
import java.time.Instant
import java.time.LocalDateTime
import java.time.ZoneId
import java.time.ZoneOffset
import java.util.*


fun interface ByteConverter<T>{
    fun toValue(bytes : ByteArray) : T
}

fun interface StringFormatter<T>{
    fun toString(value : T) : String
}

val HELSINKI_TZ = ZoneId.of("Europe/Helsinki")

val TO_UNSIGNED_INT = ByteConverter { bytes : ByteArray ->
    var tmpArray = bytes
    while(tmpArray.size < 4) tmpArray += byteArrayOf(0.toByte())
    ByteBuffer.wrap(tmpArray.toUByteArray().asByteArray()).order(java.nio.ByteOrder.LITTLE_ENDIAN).int
}

val TO_INT = ByteConverter<Int> {  bytes : ByteArray ->
    var tmpArray = bytes
    while(tmpArray.size < 4) tmpArray += byteArrayOf(0.toByte())
    ByteBuffer.wrap(tmpArray).order(java.nio.ByteOrder.LITTLE_ENDIAN).int
}

val BIG_ENDIAN_TO_INT = ByteConverter<Int> {  bytes : ByteArray ->
    var tmpArray = bytes
    while(tmpArray.size < 4) tmpArray = byteArrayOf(0.toByte()) + tmpArray
    ByteBuffer.wrap(tmpArray).order(java.nio.ByteOrder.BIG_ENDIAN).int
}

//For testing purposes
val TO_BYTE_ARRAY = ByteConverter<ByteArray> {  bytes : ByteArray -> bytes }

val TO_FLOAT = ByteConverter<Float> {  bytes : ByteArray -> ByteBuffer.wrap(bytes).order(java.nio.ByteOrder.LITTLE_ENDIAN).float }
val TO_DATE_FROM_EET = ByteConverter<Date> { bytes ->
    val rawInstant = Instant.ofEpochSecond(ByteBuffer.wrap(bytes.toUByteArray().asByteArray()).order(java.nio.ByteOrder.LITTLE_ENDIAN).int.toLong())
    val offset = HELSINKI_TZ.rules.getOffset(rawInstant)
    val ldt = LocalDateTime.ofInstant(rawInstant, ZoneOffset.UTC)
    Date(ldt.toEpochSecond(offset) * 1000L)
}

val TO_DATE =
    ByteConverter<Date> {  bytes : ByteArray -> Date(ByteBuffer.wrap(bytes.toUByteArray().asByteArray()).order(java.nio.ByteOrder.LITTLE_ENDIAN).int * 1000L ) }

val BIGENDIAN_TO_DATE =
    ByteConverter<Date> {  bytes : ByteArray -> Date(ByteBuffer.wrap(bytes.toUByteArray().asByteArray()).order(java.nio.ByteOrder.BIG_ENDIAN).int * 1000L ) }

val BIGENDIAN_TO_DATE_FROM_EET =
    ByteConverter<Date> { bytes : ByteArray ->
        val rawInstant = Instant.ofEpochSecond(ByteBuffer.wrap(bytes.toUByteArray().asByteArray()).order(java.nio.ByteOrder.BIG_ENDIAN).int.toLong())
        val offset = HELSINKI_TZ.rules.getOffset(rawInstant)
        val ldt = LocalDateTime.ofInstant(rawInstant, ZoneOffset.UTC)
        Date(ldt.toEpochSecond(offset) * 1000L)
    }

val TO_UNSIGNED_INT_ARRAY = ByteConverter<IntArray> {  bytes : ByteArray ->
    val array = IntArray(bytes.size)
    for( i in bytes.indices){
        array[i] = TO_UNSIGNED_INT.toValue(bytes.copyOfRange(i,i + 1))
    }
    array
}

val TO_STRING = ByteConverter<String> {
    bytes -> String(bytes, Charsets.US_ASCII)
}

fun <T> ByteArray.readField(field : FieldDefinition<T>) : T{
    return field.readField(this)
}

class FieldDefinition<T>{

    val offset : Int
    val size : Int
    val converter : ByteConverter<T>
    val fieldName : String
    val stringFormatter : StringFormatter<T>?

    constructor(previousField : FieldDefinition<*>?, size : Int, converter: ByteConverter<T>, fieldName: String, stringFormatter: StringFormatter<T>? = null){
        this.offset = if(previousField == null) 0 else previousField.offset + previousField.size
        this.size = size
        this.converter = converter
        this.fieldName = fieldName
        this.stringFormatter = stringFormatter
    }


    fun readField(rawData : ByteArray) : T{
        return converter.toValue(rawData.copyOfRange(offset, offset + size))
    }

    fun toString(rawData : ByteArray) : String{
        return stringFormatter?.toString(readField(rawData)) ?: readField(rawData).toString()
    }

}