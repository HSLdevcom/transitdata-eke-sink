package fi.hsl.transitdata.eke_sink

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

private const val FILE_NAME_PATTERN = "%s_%s_vehicle_%s.csv"

private val TIME_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH")

fun formatCsvFileName(messageType: String, time: LocalDateTime, unitNumber: String): String {
    return FILE_NAME_PATTERN.format(time.format(TIME_FORMATTER), messageType, unitNumber)
}