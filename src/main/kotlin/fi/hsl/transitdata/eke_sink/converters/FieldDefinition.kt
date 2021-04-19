package fi.hsl.transitdata.eke_sink.converters

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