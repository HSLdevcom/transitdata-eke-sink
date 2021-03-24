package fi.hsl.transitdata.eke_sink.messages.rmm_jkv_fault_msg

import fi.hsl.transitdata.eke_sink.converters.FieldDefinition
import fi.hsl.transitdata.eke_sink.converters.TO_INT
import fi.hsl.transitdata.eke_sink.converters.TO_STRING
import fi.hsl.transitdata.eke_sink.messages.Parser
import fi.hsl.transitdata.eke_sink.messages.mqtt_header.NTP_HUNDRED_OF_SECONDS
import fi.hsl.transitdata.eke_sink.messages.mqtt_header.headerFields
import fi.hsl.transitdata.eke_sink.messages.rmm_time_changed_data.RmmTimeChangedDataParser

val JKV_VIKATEKSTI = FieldDefinition(NTP_HUNDRED_OF_SECONDS, 8, TO_STRING, "JKV Vikatesti")
val JKV_VIKAKOODI = FieldDefinition(JKV_VIKATEKSTI, 4, TO_INT, "JKV Vikakoodi")
val RMM_JKV_FAULT_MSG_SIZE = JKV_VIKAKOODI.offset + JKV_VIKAKOODI.size

object RmmJkvFaultMsgParser : Parser(arrayOf(*headerFields, JKV_VIKATEKSTI, JKV_VIKAKOODI))