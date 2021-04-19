package fi.hsl.transitdata.eke_sink.messages.rmm_jkv_train_msg

import fi.hsl.transitdata.eke_sink.converters.FieldDefinition
import fi.hsl.transitdata.eke_sink.converters.TO_BYTE_ARRAY
import fi.hsl.transitdata.eke_sink.converters.TO_INT
import fi.hsl.transitdata.eke_sink.converters.TO_STRING
import fi.hsl.transitdata.eke_sink.messages.Parser
import fi.hsl.transitdata.eke_sink.messages.mqtt_header.NTP_HUNDRED_OF_SECONDS
import fi.hsl.transitdata.eke_sink.messages.mqtt_header.headerFields
import java.lang.reflect.Field


val UNKNOWN = FieldDefinition(NTP_HUNDRED_OF_SECONDS, 2, TO_BYTE_ARRAY, "unknown")
val SALLITTU_NOPEUS = FieldDefinition(UNKNOWN, 3, TO_STRING, "sallitu nopeus")
val JUNAN_NOPEUS = FieldDefinition(SALLITTU_NOPEUS, 4, TO_STRING, "junan nopeus")
val KOKONAISPAINO = FieldDefinition(JUNAN_NOPEUS, 4, TO_STRING, "Kokonaispaino")
val JARRUPAINO = FieldDefinition(KOKONAISPAINO, 4, TO_STRING, "Jarrupaino")
val KR_PERCENT = FieldDefinition(JARRUPAINO, 2, TO_STRING, "KR percent")
val OSA_PERCENT = FieldDefinition(KR_PERCENT, 2, TO_STRING, "OSA percent")
val PT_KOODI = FieldDefinition(OSA_PERCENT, 3, TO_STRING, "PT koodi")
val JUNA_NUMERO = FieldDefinition(PT_KOODI, 4, TO_BYTE_ARRAY, "Juna numero")

val RMM_JKV_TRAIN_MSG_SIZE = JUNA_NUMERO.offset + JUNA_NUMERO.size

object RmmJkvTrainMsgParser : Parser(arrayOf(*headerFields, SALLITTU_NOPEUS, JUNAN_NOPEUS, KOKONAISPAINO, JARRUPAINO, KR_PERCENT,
    OSA_PERCENT, PT_KOODI, JUNA_NUMERO)) {

}