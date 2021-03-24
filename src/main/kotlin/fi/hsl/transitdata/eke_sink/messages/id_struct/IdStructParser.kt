package fi.hsl.transitdata.eke_sink.messages.id_struct

import fi.hsl.transitdata.eke_sink.converters.BIG_ENDIAN_TO_INT
import fi.hsl.transitdata.eke_sink.converters.FieldDefinition
import fi.hsl.transitdata.eke_sink.messages.Parser
import fi.hsl.transitdata.eke_sink.messages.mqtt_header.NTP_HUNDRED_OF_SECONDS
import fi.hsl.transitdata.eke_sink.messages.mqtt_header.headerFields

val CABIN_NUMBER = FieldDefinition(NTP_HUNDRED_OF_SECONDS, 2, BIG_ENDIAN_TO_INT, "Cabin",  {
        value -> (((value - 5000) / 100) + 64).toChar().toString()
})
val LOCOMOTIVE_NUMBER = FieldDefinition(NTP_HUNDRED_OF_SECONDS, 2, BIG_ENDIAN_TO_INT, "Locomotive number",  {
        value -> ((value - 5000) % 100).toString()
})

val ID_STRUCT_SIZE = LOCOMOTIVE_NUMBER.offset + LOCOMOTIVE_NUMBER.size

object IdStructParser : Parser(arrayOf(*headerFields, CABIN_NUMBER, LOCOMOTIVE_NUMBER))

