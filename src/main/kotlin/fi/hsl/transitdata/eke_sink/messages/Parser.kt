package fi.hsl.transitdata.eke_sink.messages

import fi.hsl.transitdata.eke_sink.converters.FieldDefinition

abstract class Parser(val fields : Array<FieldDefinition<out Any>>) {

    open fun getFieldValues(payload : ByteArray) : Array<String>{
        return fields.map {it.toString(payload) }.toTypedArray()
    }
}