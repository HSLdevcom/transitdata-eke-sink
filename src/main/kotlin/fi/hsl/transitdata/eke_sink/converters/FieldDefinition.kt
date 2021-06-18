package fi.hsl.transitdata.eke_sink.converters

class FieldDefinition<T>(
    previousField: FieldDefinition<*>?,
    val size: Int,
    private val converter: ByteConverter<T>,
    val fieldName: String,
    private val stringFormatter: StringFormatter<T>? = null
) {
    val offset: Int = if(previousField == null) 0 else previousField.offset + previousField.size

    fun readField(rawData : ByteArray) : T{
        return converter.toValue(rawData.copyOfRange(offset, offset + size))
    }

    fun toString(rawData : ByteArray) : String{
        return stringFormatter?.toString(readField(rawData)) ?: readField(rawData).toString()
    }
}