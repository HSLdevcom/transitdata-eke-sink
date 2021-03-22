package fi.hsl.transitdata.eke_sink.messages.rmm_jkv_beacon

import fi.hsl.transitdata.eke_sink.converters.FieldDefinition
import fi.hsl.transitdata.eke_sink.converters.TO_INT
import fi.hsl.transitdata.eke_sink.messages.mqtt_header.NTP_HUNDRED_OF_SECONDS
import fi.hsl.transitdata.eke_sink.messages.mqtt_header.headerFields
import fi.hsl.transitdata.eke_sink.messages.rmm_jkv_fault_msg.RmmJkvFaultMsgParser

//TODO this one will need a custim parser
object RmmJkvBeaconParser {
    val TRANSPONDER_PART = FieldDefinition(NTP_HUNDRED_OF_SECONDS, 1, TO_INT, "TransponderPart")
    val CRC_PART = FieldDefinition(TRANSPONDER_PART, 1, TO_INT, "CRC")
    val TRANSPONDER_TELEGRAM_CATEGORY = FieldDefinition(CRC_PART, 1, TO_INT, "TransponderTelegramCategory")
    val RMM_JKV_BEACON_SIZE = TRANSPONDER_TELEGRAM_CATEGORY.offset + TRANSPONDER_TELEGRAM_CATEGORY.size

    val fields = arrayOf(*headerFields, TRANSPONDER_PART, CRC_PART)
}