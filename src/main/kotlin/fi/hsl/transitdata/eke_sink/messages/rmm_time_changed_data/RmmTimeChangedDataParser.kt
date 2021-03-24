package fi.hsl.transitdata.eke_sink.messages.rmm_time_changed_data

import fi.hsl.transitdata.eke_sink.converters.FieldDefinition
import fi.hsl.transitdata.eke_sink.converters.TO_DATE
import fi.hsl.transitdata.eke_sink.converters.TO_INT
import fi.hsl.transitdata.eke_sink.messages.Parser
import fi.hsl.transitdata.eke_sink.messages.mqtt_header.NTP_HUNDRED_OF_SECONDS
import fi.hsl.transitdata.eke_sink.messages.mqtt_header.headerFields
import fi.hsl.transitdata.eke_sink.messages.rmm_painerajavirhe12.RmmPaineRajaVirhe12

val NEW_DATE = FieldDefinition(NTP_HUNDRED_OF_SECONDS, 4, TO_DATE, "New date")
val OLD_DATE = FieldDefinition(NEW_DATE, 4, TO_DATE, "Old date")
val RMM_TIME_CHANDED_DATA_SIZE = OLD_DATE.offset + OLD_DATE.size
object RmmTimeChangedDataParser : Parser(arrayOf(*headerFields,
    NEW_DATE,
    OLD_DATE
)) {

}