package fi.hsl.transitdata.eke_sink.messages.rmm_jkv_beacon

import fi.hsl.transitdata.eke_sink.JSON_FILE_NAME_PATTERN
import fi.hsl.transitdata.eke_sink.converters.FieldDefinition
import fi.hsl.transitdata.eke_sink.converters.TO_BYTE_ARRAY
import fi.hsl.transitdata.eke_sink.converters.TO_INT
import fi.hsl.transitdata.eke_sink.messages.Parser
import fi.hsl.transitdata.eke_sink.messages.mqtt_header.NTP_HUNDRED_OF_SECONDS
import fi.hsl.transitdata.eke_sink.messages.mqtt_header.headerFields
import java.io.File

//TODO this one will need a custim parser
val BALIISI = FieldDefinition(NTP_HUNDRED_OF_SECONDS, 16, TO_BYTE_ARRAY, "Baliisi")

val TRANSPONDER_PART = FieldDefinition(NTP_HUNDRED_OF_SECONDS, 1, TO_INT, "TransponderPart")
val CRC_PART = FieldDefinition(TRANSPONDER_PART, 1, TO_INT, "CRC")
val TRANSPONDER_TELEGRAM_CATEGORY = FieldDefinition(CRC_PART, 1, TO_INT, "TransponderTelegramCategory")
val RMM_JKV_BEACON_SIZE = TRANSPONDER_TELEGRAM_CATEGORY.offset + TRANSPONDER_TELEGRAM_CATEGORY.size

private val PATH = File("eke")

object RmmJkvBeaconParser : Parser(arrayOf(*headerFields, TRANSPONDER_PART, CRC_PART)){

    var i = 0
    override fun getFieldValues(payload : ByteArray) : Array<String>{
        val file = File(PATH, "rmmjkvbeacon" + i + ".dat")
        if(!file.exists()) file.createNewFile()
        file.appendBytes(payload)
        i++
        return arrayOf("")
    }

}