package fi.hsl.transitdata.eke_sink.messages.rmm_io_struct

import fi.hsl.transitdata.eke_sink.converters.*
import fi.hsl.transitdata.eke_sink.messages.mqtt_header.NTP_HUNDRED_OF_SECONDS
import fi.hsl.transitdata.eke_sink.messages.mqtt_header.headerFields
import fi.hsl.transitdata.eke_sink.messages.rmm_jkv_beacon.RmmJkvBeaconParser

object RmmIoStructParser {

    val BIT_ARRAY_1 = FieldDefinition(NTP_HUNDRED_OF_SECONDS, 1, TO_UNSIGNED_INT, "bitArray1")
    val BIT_ARRAY_2 = FieldDefinition(BIT_ARRAY_1, 1, TO_UNSIGNED_INT, "bitArray2")
    val JARRUPAINE = FieldDefinition(BIT_ARRAY_2, 2, TO_INT, "jarruPaine", {
        value -> (value.toDouble() / 100).toString()
    })
    val UNUSED = FieldDefinition(JARRUPAINE, 11, TO_BYTE_ARRAY, "unused")
    val AC_KIIHTYVYYS = FieldDefinition(UNUSED, 2, TO_INT, "acKiihtyvyys", {
        value -> (value.toDouble() / 1000).toString()
    })
    val OLONOPEUS = FieldDefinition(AC_KIIHTYVYYS, 2, TO_UNSIGNED_INT, "olonopeus")
    val RMM_IO_STRUCT_SIZE = OLONOPEUS.offset + OLONOPEUS.size

    val fields = arrayOf(*headerFields, BIT_ARRAY_1, BIT_ARRAY_2, JARRUPAINE, AC_KIIHTYVYYS, OLONOPEUS)
}