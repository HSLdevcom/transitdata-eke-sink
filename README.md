# transitdata-eke-sink [![Test and create Docker image](https://github.com/HSLdevcom/transitdata-eke-sink/actions/workflows/test-and-build.yml/badge.svg)](https://github.com/HSLdevcom/transitdata-eke-sink/actions/workflows/test-and-build.yml)

Application for parsing EKE data messages, storing them in compressed CSV files and uploading the CSV files to blob storage. This application is part of [Transitdata](https://github.com/HSLdevcom/transitdata).

## Data format

The CSV format that is used by the files that this application produces is following:

`message_type,ntp_timestamp,ntp_ok,eke_timestamp,mqtt_timestamp,mqtt_topic,raw_data`

The fields are described below:

| Field            | Data format | Description
| ---------------- | ----------- | ------------
| `message_type`   | Integer     | Message type, see EKE documentation for possible values. `99` for connection status messages
| `ntp_timestamp`  | ISO 8601    | NTP timestamp. Not available for connection status messages.
| `ntp_ok`         | Boolean     | Whether the NTP time was synchronized or not. Not available for connection status messages.
| `eke_timestamp`  | ISO 8601    | EKE timestamp, local time.
| `mqtt_timestamp` | ISO 8601    | Time when the MQTT message was received.
| `mqtt_topic`     | String      | MQTT topic where the message was sent to.
| `raw_data`       | String      | Raw MQTT payload, including the 12 byte header, encoded as hexadecimal string. Connection status in human-readable format for connection status messages.