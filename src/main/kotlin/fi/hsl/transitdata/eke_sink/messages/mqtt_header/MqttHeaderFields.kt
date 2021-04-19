package fi.hsl.transitdata.eke_sink.messages.mqtt_header

import fi.hsl.transitdata.eke_sink.converters.*

val MQTT_HEADER_1ST_PART = FieldDefinition(null , 2, TO_INT, "header")
val EKE_TIME = FieldDefinition(MQTT_HEADER_1ST_PART, 4, BIGENDIAN_TO_DATE_FROM_EET, "EKETime")
val EKE_HUNDRED_OF_SECONDS = FieldDefinition(EKE_TIME, 1, BIG_ENDIAN_TO_INT,"hundredsOfSeconds")
val NTP_TIME = FieldDefinition(EKE_HUNDRED_OF_SECONDS, 4, BIGENDIAN_TO_DATE, "NTPTime")
val NTP_HUNDRED_OF_SECONDS = FieldDefinition(NTP_TIME, 1, BIG_ENDIAN_TO_INT,"NTPHundredsOfSeconds")

val headerFields = arrayOf(MQTT_HEADER_1ST_PART, EKE_TIME, EKE_HUNDRED_OF_SECONDS, NTP_TIME,NTP_HUNDRED_OF_SECONDS)
