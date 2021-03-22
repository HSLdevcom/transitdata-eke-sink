package fi.hsl.transitdata.eke_sink.messages.rmm_painerajavirhe12

import fi.hsl.transitdata.eke_sink.converters.FieldDefinition
import fi.hsl.transitdata.eke_sink.converters.TO_BYTE_ARRAY
import fi.hsl.transitdata.eke_sink.converters.TO_INT
import fi.hsl.transitdata.eke_sink.messages.mqtt_header.NTP_HUNDRED_OF_SECONDS
import fi.hsl.transitdata.eke_sink.messages.mqtt_header.headerFields
import fi.hsl.transitdata.eke_sink.messages.rmm_jkv_train_msg.RmmJkvTrainMsgParser

object RmmPaineRajaVirhe12 {

    val UNKNOWN = FieldDefinition(NTP_HUNDRED_OF_SECONDS, 10, TO_BYTE_ARRAY, "unknown")
    val PAINE_RAJA_VIRHE12 = FieldDefinition(UNKNOWN, 1, TO_INT, "paineRajeVirhe12")
    val RMM_PAINE_RAJA_VIRHE_12_SIZE = PAINE_RAJA_VIRHE12.offset + PAINE_RAJA_VIRHE12.size

    val fields = arrayOf(*headerFields, PAINE_RAJA_VIRHE12)
}