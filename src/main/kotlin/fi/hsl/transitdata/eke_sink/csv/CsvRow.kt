package fi.hsl.transitdata.eke_sink.csv

import fi.hsl.transitdata.eke_sink.messages.ConnectionStatus
import fi.hsl.transitdata.eke_sink.messages.MqttHeader
import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter

data class CsvRow(val messageType: Int, val ntpTime: ZonedDateTime?, val ntpOk: Boolean?, val ekeTime: ZonedDateTime?, val mqttTime: ZonedDateTime, val mqttTopic: String, val rawData: String) {
    companion object {
        val CSV_HEADER = listOf("message_type", "ntp_timestamp", "ntp_ok", "eke_timestamp", "mqtt_timestamp", "mqtt_topic", "raw_data")

        fun create(mqttTopic: String, mqttTime: ZonedDateTime, mqttHeader: MqttHeader, rawData: String): CsvRow =
            CsvRow(mqttHeader.messageType,
                mqttHeader.ntpTimeExact,
                mqttHeader.ntpValid,
                mqttHeader.ekeTimeExact,
                mqttTime,
                mqttTopic,
                rawData)

        fun create(mqttTopic: String, mqttTime: ZonedDateTime, connectionStatus: ConnectionStatus): CsvRow =
            CsvRow(ConnectionStatus.CONNECTION_STATUS_MESSAGE_TYPE,
                null,
                null,
                connectionStatus.timestamp,
                mqttTime,
                mqttTopic,
                connectionStatus.status)
    }

    fun asList(): List<String> {
        return listOf(
            messageType.toString(),
            ntpTime?.format(DateTimeFormatter.ISO_OFFSET_DATE_TIME) ?: "",
            ntpOk?.toString() ?: "",
            ekeTime?.format(DateTimeFormatter.ISO_OFFSET_DATE_TIME) ?: "",
            mqttTime.format(DateTimeFormatter.ISO_OFFSET_DATE_TIME),
            mqttTopic,
            rawData
        )
    }
}
