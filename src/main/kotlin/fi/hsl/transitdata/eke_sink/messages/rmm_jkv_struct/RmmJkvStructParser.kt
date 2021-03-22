package fi.hsl.transitdata.eke_sink.messages.rmm_jkv_struct

import fi.hsl.transitdata.eke_sink.converters.*
import fi.hsl.transitdata.eke_sink.messages.mqtt_header.NTP_HUNDRED_OF_SECONDS
import fi.hsl.transitdata.eke_sink.messages.mqtt_header.headerFields

object RmmJkvStructParser {

    val TAVOITENOPEUS = FieldDefinition(NTP_HUNDRED_OF_SECONDS, 1, TO_INT, "tavoitenopeus")
    val JKV_NOPEUS = FieldDefinition(TAVOITENOPEUS, 1, TO_INT, "JKVNopeus" )
    val JKV_JARRUPAINE = FieldDefinition(JKV_NOPEUS, 1, TO_INT, "JKVJarrupaine", {
        value -> (value.toDouble() / 10).toString()
    })
    val NOPEUSERO = FieldDefinition(JKV_JARRUPAINE, 1, TO_INT, "Nopeusero")
    val EI_KAYTOSSA = FieldDefinition(NOPEUSERO, 1, TO_BYTE_ARRAY, "EiKaytossa")
    val BIT_ARRAY_1 = FieldDefinition(EI_KAYTOSSA, 1, TO_UNSIGNED_INT, "BitArray1")
    val BIT_ARRAY_2 = FieldDefinition(BIT_ARRAY_1, 1, TO_UNSIGNED_INT, "BitArray2")
    val BIT_ARRAY_3 = FieldDefinition(BIT_ARRAY_2, 1, TO_UNSIGNED_INT, "BitArray3")
    val RMM_JKV_STRUCT_SIZE = BIT_ARRAY_3.offset + BIT_ARRAY_3.size

    val fields = arrayOf(*headerFields,  TAVOITENOPEUS, JKV_NOPEUS, JKV_JARRUPAINE, NOPEUSERO, BIT_ARRAY_1, BIT_ARRAY_2, BIT_ARRAY_3)
}