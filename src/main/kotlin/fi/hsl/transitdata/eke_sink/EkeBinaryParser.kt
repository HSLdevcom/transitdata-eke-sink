package fi.hsl.transitdata.eke_sink

import org.apache.commons.csv.CSVFormat
import org.apache.commons.csv.CSVPrinter
import kotlin.math.floor

object EkeBinaryParser {


    val MQTT_HEADER_1ST_PART = FieldDefinition(MQTT_HEADER_1ST_PART_OFFSET, MQTT_HEADER_1ST_PART_SIZE, TO_INT, "header")
    val EKE_TIME = FieldDefinition(EKE_TIME_OFFSET, EKE_TIME_SIZE, BIGENDIAN_TO_DATE, "EKETime")
    val EKE_HUNDRED_OF_SECONDS = FieldDefinition(EKE_HUNDRED_OF_SECONDS_OFFSET, EKE_HUNDRED_OF_SECONDS_SIZE, BIG_ENDIAN_TO_INT, "hundredsOfSeconds")
    val NTP_TIME = FieldDefinition(NTP_TIME_OFFSET, NTP_TIME_SIZE, BIGENDIAN_TO_DATE, "NTPTime")
    val NTP_HUNDRED_OF_SECONDS = FieldDefinition(NTP_HUNDRED_OF_SECONDS_OFFSET, NTP_HUNDRED_OF_SECONDS_SIZE, BIG_ENDIAN_TO_INT, "NTPHundredsOfSeconds")
    //val TIMESTAMP = FieldDefinition(TIMESTAMP_OFFSET, TIMESTAMP_SIZE, TO_DATE, "timestamp") Timestamp only in the test file
    val INDEX = FieldDefinition(INDEX_OFFSET, INDEX_SIZE, TO_UNSIGNED_INT, "index")
    val RESERVED1 = FieldDefinition(RESERVED1_OFFSET, RESERVED1_SIZE, TO_UNSIGNED_INT, "reserved1")
    val VEHICLE_SHUTTING_DOWN = FieldDefinition(VEHICLE_SHUTTING_DOWN_OFFSET, VEHICLE_SHUTTING_DOWN_SIZE, TO_UNSIGNED_INT,"vehicleShuttingDown")
    val RESERVED2 = FieldDefinition(RESERVED2_OFFSET, RESERVED2_SIZE, TO_UNSIGNED_INT, "reserved2")
    val SPEED = FieldDefinition(SPEED_OFFSET, SPEED_SIZE, TO_FLOAT, "speed")
    val ODOMETER = FieldDefinition(ODOMETER_OFFSET, ODOMETER_SIZE, TO_UNSIGNED_INT, "odometer")
    val RESERVED3 = FieldDefinition(RESERVED3_OFFSET, RESERVED3_SIZE, TO_UNSIGNED_INT, "reserved3")
    val NUMBER_OF_KILOMETERS = FieldDefinition(NUMBER_OF_KILOMETERS_OFFSET, NUMBER_OF_KILOMETERS_SIZE, TO_UNSIGNED_INT, "numberOfKilometers")
    val ACCELERATION = FieldDefinition(ACCELERATION_OFFSET, ACCELERATION_SIZE, TO_FLOAT, "acceleration")
    val STANDSTILL = FieldDefinition(STANDSTILL_OFFSET, STANDSTILL_SIZE, TO_UNSIGNED_INT,"standstill")
    val DOORS_L_OPEN_VEHICLE1 = FieldDefinition(DOORS_L_OPEN_VEHICLE1_OFFSET, DOORS_L_OPEN_VEHICLE1_SIZE, TO_UNSIGNED_INT, "doorsLOpenVehicle1")
    val DOORS_R_OPEN_VEHICLE1 = FieldDefinition(DOORS_R_OPEN_VEHICLE1_OFFSET, DOORS_R_OPEN_VEHICLE1_SIZE, TO_UNSIGNED_INT, "doorsROpenVehicle1")
    val DOORS_L_OPEN_VEHICLE2 = FieldDefinition(DOORS_L_OPEN_VEHICLE2_OFFSET, DOORS_L_OPEN_VEHICLE2_SIZE, TO_UNSIGNED_INT, "doorsLOpenVehicle2")
    val DOORS_R_OPEN_VEHICLE2 = FieldDefinition(DOORS_R_OPEN_VEHICLE2_OFFSET, DOORS_R_OPEN_VEHICLE2_SIZE, TO_UNSIGNED_INT, "doorsROpenVehicle2")
    val DOORS_L_OPEN_VEHICLE3 = FieldDefinition(DOORS_L_OPEN_VEHICLE3_OFFSET, DOORS_L_OPEN_VEHICLE3_SIZE, TO_UNSIGNED_INT,"doorsLOpenVehicle3")
    val DOORS_R_OPEN_VEHICLE3 = FieldDefinition(DOORS_R_OPEN_VEHICLE3_OFFSET, DOORS_R_OPEN_VEHICLE3_SIZE, TO_UNSIGNED_INT,"doorsROpenVehicle3")
    val DOORS_L_OPEN_VEHICLE4 = FieldDefinition(DOORS_L_OPEN_VEHICLE4_OFFSET, DOORS_L_OPEN_VEHICLE4_SIZE, TO_UNSIGNED_INT,"doorsLOpenVehicle4")
    val DOORS_R_OPEN_VEHICLE4 = FieldDefinition(DOORS_R_OPEN_VEHICLE4_OFFSET, DOORS_R_OPEN_VEHICLE4_SIZE, TO_UNSIGNED_INT, "doorsROpenVehicle4")
    val ACTIVE_SETPOINT_SPEED = FieldDefinition(ACTIVE_SETPOINT_SPEED_OFFSET, ACTIVE_SETPOINT_SPEED_SIZE, TO_UNSIGNED_INT, "activeSetpointSpeed")
    val RESERVED4 = FieldDefinition(RESERVED4_OFFSET, RESERVED4_SIZE, TO_UNSIGNED_INT, "reserved4")
    val RESERVED5 = FieldDefinition(RESERVED5_OFFSET, RESERVED5_SIZE, TO_UNSIGNED_INT,"reserved5")
    val TRACTION_LEVER_POSITION = FieldDefinition(TRACTION_LEVER_POSITION_OFFSET, TRACTION_LEVER_POSITION_SIZE, TO_FLOAT, "tractionLeverPosition")
    val TRACTION_SET_POINT = FieldDefinition(TRACTION_SET_POINT_OFFSET, TRACTION_SET_POINT_SIZE, TO_FLOAT, "tractionSetPoint")
    val TRACTION_ACTUAL = FieldDefinition(TRACTION_ACTUAL_OFFSET, TRACTION_ACTUAL_SIZE, TO_FLOAT, "tractionActual")
    val TRACTION_MOTOR_1A_POWER = FieldDefinition(TRACTION_MOTOR_1A_POWER_OFFSET, TRACTION_MOTOR_1A_POWER_SIZE, TO_FLOAT, "tractionMotor1APower")
    val TRACTION_MOTOR_1B_POWER = FieldDefinition(TRACTION_MOTOR_1B_POWER_OFFSET, TRACTION_MOTOR_1B_POWER_SIZE, TO_FLOAT, "tractionMotor1BPower")
    val TRACTION_MOTOR_2A_POWER = FieldDefinition(TRACTION_MOTOR_2A_POWER_OFFSET, TRACTION_MOTOR_2A_POWER_SIZE, TO_FLOAT, "tractionMotor2APower")
    val TRACTION_MOTOR_2B_POWER = FieldDefinition(TRACTION_MOTOR_2B_POWER_OFFSET, TRACTION_MOTOR_2B_POWER_SIZE, TO_FLOAT,"tractionMotor2BPower")
    val TRACTION_MOTOR_1A_TEMP = FieldDefinition(TRACTION_MOTOR_1A_TEMP_OFFSET, TRACTION_MOTOR_1A_TEMP_SIZE, TO_FLOAT, "tractionMotor1ATemp")
    val TRACTION_MOTOR_1B_TEMP = FieldDefinition(TRACTION_MOTOR_1B_TEMP_OFFSET, TRACTION_MOTOR_1B_TEMP_SIZE, TO_FLOAT, "tractionMotor1BTemp")
    val TRACTION_MOTOR_2B_TEMP = FieldDefinition(TRACTION_MOTOR_2B_TEMP_OFFSET, TRACTION_MOTOR_2B_TEMP_SIZE, TO_FLOAT, "tractionMotor2BTemp")
    val TRACTION_MOTOR_2A_TEMP = FieldDefinition(TRACTION_MOTOR_2A_TEMP_OFFSET, TRACTION_MOTOR_2A_TEMP_SIZE, TO_FLOAT, "tractionMotor2ATemp")
    val POWER_CONVERTER_1A_COOLING_WATER_TEMP = FieldDefinition(POWER_CONVERTER_1A_COOLING_WATER_TEMP_OFFSET, POWER_CONVERTER_1A_COOLING_WATER_TEMP_SIZE, TO_FLOAT, "powerConverter1ACoolingWaterTemp")
    val POWER_CONVERTER_1B_COOLING_WATER_TEMP = FieldDefinition(POWER_CONVERTER_1B_COOLING_WATER_TEMP_OFFSET, POWER_CONVERTER_1B_COOLING_WATER_TEMP_SIZE, TO_FLOAT, "powerConverter1BCoolingWaterTemp")
    val POWER_CONVERTER_2A_COOLING_WATER_TEMP = FieldDefinition(POWER_CONVERTER_2A_COOLING_WATER_TEMP_OFFSET, POWER_CONVERTER_2A_COOLING_WATER_TEMP_SIZE, TO_FLOAT, "powerConverter2ACoolingWaterTemp")
    val POWER_CONVERTER_2B_COOLING_WATER_TEMP = FieldDefinition(POWER_CONVERTER_2B_COOLING_WATER_TEMP_OFFSET, POWER_CONVERTER_2B_COOLING_WATER_TEMP_SIZE, TO_FLOAT, "powerConverter2BCoolingWaterTemp")
    val MAIN_BRAKE_PIPE_PRESSURE = FieldDefinition(MAIN_BRAKE_PIPE_PRESSURE_OFFSET, MAIN_BRAKE_PIPE_PRESSURE_SIZE, TO_FLOAT, "mainBrakePipePressure")
    val BRAKE_CYLINDER_PRESSURE_AT_AXLE = FieldDefinition(BRAKE_CYLINDER_PRESSURE_AT_AXLE_OFFSET, BRAKE_CYLINDER_PRESSURE_AT_AXLE_SIZE, TO_UNSIGNED_INT_ARRAY, "breakCylinderPressureAtAxle", StringFormatter{ value -> value.map { it / 10f }.joinToString(",","[","]") })
    val WHEEL_SLIPPAGE_PROTECTION_ACTIVE = FieldDefinition(WHEEL_SLIPPAGE_PROTECTION_ACTIVE_OFFSET, WHEEL_SLIPPAGE_PROTECTION_ACTIVE_SIZE, TO_UNSIGNED_INT,"wheelSlippageProtectionActive")
    val SLIPPAGE_1A = FieldDefinition(SLIPPAGE_1A_OFFSET, SLIPPAGE_1A_SIZE, TO_FLOAT,"slippage1A")
    val SLIPPAGE_1B = FieldDefinition(SLIPPAGE_1B_OFFSET, SLIPPAGE_1B_SIZE, TO_FLOAT,"slippage1B")
    val SLIPPAGE_2A = FieldDefinition(SLIPPAGE_2A_OFFSET, SLIPPAGE_2A_SIZE, TO_FLOAT,"slippage2A")
    val SLIPPAGE_2B = FieldDefinition(SLIPPAGE_2B_OFFSET, SLIPPAGE_2B_SIZE, TO_FLOAT,"slippage2B")
    val ENERGY_CONSUMPTION = FieldDefinition(ENERGY_CONSUMPTION_OFFSET, ENERGY_CONSUMPTION_SIZE, TO_UNSIGNED_INT,"energyConsumption")
    val ENERGY_RECUPERATION = FieldDefinition(ENERGY_RECUPERATION_OFFSET, ENERGY_RECUPERATION_SIZE, TO_UNSIGNED_INT,"energyRecuperation")
    val CATENARY_VOLTAGE = FieldDefinition(CATENARY_VOLTAGE_OFFSET, CATENARY_VOLTAGE_SIZE, TO_UNSIGNED_INT,"catenaryVoltage", StringFormatter { value -> (value / 10f).toString() })
    val INSIDE_TEMP_COACH_A = FieldDefinition(INSIDE_TEMP_COACH_A_OFFSET, INSIDE_TEMP_COACH_A_SIZE, TO_UNSIGNED_INT,"insideTempCoachA")
    val INSIDE_TEMP_COACH_B = FieldDefinition(INSIDE_TEMP_COACH_B_OFFSET, INSIDE_TEMP_COACH_B_SIZE, TO_UNSIGNED_INT,"insideTempCoachB")
    val INSIDE_TEMP_COACH_C = FieldDefinition(INSIDE_TEMP_COACH_C_OFFSET, INSIDE_TEMP_COACH_C_SIZE, TO_UNSIGNED_INT,"insideTempCoachC")
    val INSIDE_TEMP_COACH_D = FieldDefinition(INSIDE_TEMP_COACH_D_OFFSET, INSIDE_TEMP_COACH_D_SIZE, TO_UNSIGNED_INT,"insideTempCoachD")
    val TOILET_FRESH_WATER_LEVEL = FieldDefinition(TOILET_FRESH_WATER_LEVEL_OFFSET, TOILET_FRESH_WATER_LEVEL_SIZE, TO_UNSIGNED_INT,"toiletFreshWaterLevel")
    val TOILET_WASTE_WATER_LEVEL = FieldDefinition(TOILET_WASTE_WATER_LEVEL_OFFSET, TOILET_WASTE_WATER_LEVEL_SIZE, TO_UNSIGNED_INT,"toiletWasteWaterLevel")
    val FLAGS = FieldDefinition(FLAGS_OFFSET, FLAGS_SIZE, TO_UNSIGNED_INT_ARRAY,"flags",
        { value -> value.joinToString(",","[","]") })
    val NUMBER_OF_VEHICLES = FieldDefinition(NUMBER_OF_VEHICLES_OFFSET, NUMBER_OF_VEHICLES_SIZE, TO_UNSIGNED_INT,"numberOfVehicles")
    val VEHICLE_POS_IN_TRAIN = FieldDefinition(VEHICLE_POS_IN_TRAIN_OFFSET, VEHICLE_POS_IN_TRAIN_SIZE, TO_UNSIGNED_INT,"vehiclePosInTrain")
    val VEHICLE_NUMBER = FieldDefinition(VEHICLE_NUMBER_OFFSET, VEHICLE_NUMBER_SIZE, TO_UNSIGNED_INT_ARRAY,"vehicleNumber",
        { value -> value.joinToString(",","[","]") })
    val ORIENTATION_VEHICLE = FieldDefinition(ORIENTATION_VEHICLE_OFFSET, ORIENTATION_VEHICLE_SIZE, TO_UNSIGNED_INT,"orientationVehicle")
    val EMERGENCY_BRAKE = FieldDefinition(EMERGENCY_BRAKE_OFFSET, EMERGENCY_BRAKE_SIZE, TO_UNSIGNED_INT,"emergencyBrake")
    val SHOW_IMAGE_OF_CAMERA_N = FieldDefinition(SHOW_IMAGE_OF_CAMERA_N_OFFSET, SHOW_IMAGE_OF_CAMERA_N_SIZE, TO_UNSIGNED_INT,"showImageOfCameraN")
    val SHOW_IMAGE_OF_VEHICLE_N = FieldDefinition(SHOW_IMAGE_OF_VEHICLE_N_OFFSET, SHOW_IMAGE_OF_VEHICLE_N_SIZE, TO_UNSIGNED_INT,"showImageOfVehicleN")
    val OUTSIDE_TEMP = FieldDefinition(OUTSIDE_TEMP_OFFSET, OUTSIDE_TEMP_SIZE, TO_INT,"outsideTemp",  { value -> (value / 10f).toString() })
    val TRAIN_NUMBER = FieldDefinition(TRAIN_NUMBER_OFFSET, TRAIN_NUMBER_SIZE, TO_UNSIGNED_INT,"trainNumber")
    val GPS_SPEED = FieldDefinition(GPS_SPEED_OFFSET, GPS_SPEED_SIZE, TO_UNSIGNED_INT,"GPSSpeed")
    val RESERVED6 = FieldDefinition(RESERVED6_OFFSET, RESERVED6_SIZE, TO_UNSIGNED_INT,"reserved6")
    val GPSX = FieldDefinition(GPSX_OFFSET, GPSX_SIZE, TO_FLOAT,"GPSX", { value -> (NMEAToGPS(value.toDouble()).toString())})
    val GPSY = FieldDefinition(GPSY_OFFSET, GPSY_SIZE, TO_FLOAT,"GPSY", { value -> (NMEAToGPS(value.toDouble()).toString())})
    //The timestamp of the GPS time is based on the EET timezone. This is not standard so we should substract the difference between UTC and EET timezones
    val TIME = FieldDefinition(TIME_OFFSET, TIME_SIZE, TO_DATE_FROM_EET,"GPSTime")


    val fields = arrayOf(MQTT_HEADER_1ST_PART, EKE_TIME, EKE_HUNDRED_OF_SECONDS, NTP_TIME, NTP_HUNDRED_OF_SECONDS, INDEX,
        RESERVED1, VEHICLE_SHUTTING_DOWN, RESERVED2, SPEED, ODOMETER, RESERVED3, NUMBER_OF_KILOMETERS, ACCELERATION, STANDSTILL,
        DOORS_L_OPEN_VEHICLE1, DOORS_R_OPEN_VEHICLE1, DOORS_L_OPEN_VEHICLE2, DOORS_R_OPEN_VEHICLE2, DOORS_L_OPEN_VEHICLE3,
        DOORS_R_OPEN_VEHICLE3, DOORS_L_OPEN_VEHICLE4, DOORS_R_OPEN_VEHICLE4, ACTIVE_SETPOINT_SPEED, RESERVED4, RESERVED5,
        TRACTION_LEVER_POSITION, TRACTION_SET_POINT, TRACTION_ACTUAL, TRACTION_MOTOR_1A_POWER, TRACTION_MOTOR_1B_POWER,
        TRACTION_MOTOR_2A_POWER, TRACTION_MOTOR_2B_POWER, TRACTION_MOTOR_1A_TEMP, TRACTION_MOTOR_1B_TEMP, TRACTION_MOTOR_2B_TEMP,
        TRACTION_MOTOR_2A_TEMP, POWER_CONVERTER_1A_COOLING_WATER_TEMP, POWER_CONVERTER_1B_COOLING_WATER_TEMP,
        POWER_CONVERTER_2A_COOLING_WATER_TEMP, POWER_CONVERTER_2B_COOLING_WATER_TEMP, MAIN_BRAKE_PIPE_PRESSURE, BRAKE_CYLINDER_PRESSURE_AT_AXLE,
        WHEEL_SLIPPAGE_PROTECTION_ACTIVE, SLIPPAGE_1A, SLIPPAGE_1B, SLIPPAGE_2A, SLIPPAGE_2B, ENERGY_CONSUMPTION, ENERGY_RECUPERATION,
        CATENARY_VOLTAGE, INSIDE_TEMP_COACH_A, INSIDE_TEMP_COACH_B, INSIDE_TEMP_COACH_C, INSIDE_TEMP_COACH_D, TOILET_FRESH_WATER_LEVEL,
        TOILET_WASTE_WATER_LEVEL, FLAGS, NUMBER_OF_VEHICLES, VEHICLE_POS_IN_TRAIN, VEHICLE_NUMBER, ORIENTATION_VEHICLE,
        EMERGENCY_BRAKE, SHOW_IMAGE_OF_CAMERA_N, SHOW_IMAGE_OF_VEHICLE_N, OUTSIDE_TEMP, TRAIN_NUMBER, GPS_SPEED, RESERVED6,
        GPSX, GPSY, TIME
        )


    fun toJson(payload : ByteArray) : String {
        return "{" +
            """"${MQTT_HEADER_1ST_PART.jsonFieldName}" : ${payload.readField(MQTT_HEADER_1ST_PART)},""" +
            """"${EKE_TIME.jsonFieldName}" : "${payload.readField(EKE_TIME)}",""" +
            """"${EKE_HUNDRED_OF_SECONDS.jsonFieldName}" : ${payload.readField(EKE_HUNDRED_OF_SECONDS)},""" +
            """"${NTP_TIME.jsonFieldName}" : "${payload.readField(NTP_TIME)}",""" +
            """"${NTP_HUNDRED_OF_SECONDS.jsonFieldName}" : ${payload.readField(NTP_HUNDRED_OF_SECONDS)},""" +
            //""""${TIMESTAMP.jsonFieldName}" : "${payload.readField(TIMESTAMP)}",""" +
            """"${INDEX.jsonFieldName}" : ${payload.readField(INDEX)},""" +
            """"${RESERVED1.jsonFieldName}" : ${payload.readField(RESERVED1)},""" +
            """"${VEHICLE_SHUTTING_DOWN.jsonFieldName}" : ${payload.readField(VEHICLE_SHUTTING_DOWN)},""" +
            """"${RESERVED2.jsonFieldName}" : ${payload.readField(RESERVED2)},""" +
            """"${SPEED.jsonFieldName}" : ${payload.readField(SPEED)},""" +
            """"${ODOMETER.jsonFieldName}" : ${payload.readField(ODOMETER)},""" +
            """"${RESERVED3.jsonFieldName}" : ${payload.readField(RESERVED3)},""" +
            """"${NUMBER_OF_KILOMETERS.jsonFieldName}" : ${payload.readField(NUMBER_OF_KILOMETERS)},""" +
            """"${ACCELERATION.jsonFieldName}" : ${payload.readField(ACCELERATION)},""" +
            """"${STANDSTILL.jsonFieldName}" : ${payload.readField(STANDSTILL)},""" +
            """"${DOORS_L_OPEN_VEHICLE1.jsonFieldName}" : ${payload.readField(DOORS_L_OPEN_VEHICLE1)},""" +
            """"${DOORS_R_OPEN_VEHICLE1.jsonFieldName}" : ${payload.readField(DOORS_R_OPEN_VEHICLE1)},""" +
            """"${DOORS_L_OPEN_VEHICLE2.jsonFieldName}" : ${payload.readField(DOORS_L_OPEN_VEHICLE2)},""" +
            """"${DOORS_R_OPEN_VEHICLE2.jsonFieldName}" : ${payload.readField(DOORS_R_OPEN_VEHICLE2)},""" +
            """"${DOORS_L_OPEN_VEHICLE3.jsonFieldName}" : ${payload.readField(DOORS_L_OPEN_VEHICLE3)},""" +
            """"${DOORS_R_OPEN_VEHICLE3.jsonFieldName}" : ${payload.readField(DOORS_R_OPEN_VEHICLE3)},""" +
            """"${DOORS_L_OPEN_VEHICLE4.jsonFieldName}" : ${payload.readField(DOORS_L_OPEN_VEHICLE4)},""" +
            """"${DOORS_R_OPEN_VEHICLE4.jsonFieldName}" : ${payload.readField(DOORS_R_OPEN_VEHICLE4)},""" +
            """"${ACTIVE_SETPOINT_SPEED.jsonFieldName}" : ${payload.readField(ACTIVE_SETPOINT_SPEED)},""" +
            """"${RESERVED4.jsonFieldName}" : ${payload.readField(RESERVED4)},""" +
            """"${RESERVED5.jsonFieldName}" : ${payload.readField(RESERVED5)},""" +
            """"${TRACTION_LEVER_POSITION.jsonFieldName}" : ${payload.readField(TRACTION_LEVER_POSITION)},""" +
            """"${TRACTION_SET_POINT.jsonFieldName}" : ${payload.readField(TRACTION_SET_POINT)},""" +
            """"${TRACTION_ACTUAL.jsonFieldName}" : ${payload.readField(TRACTION_ACTUAL)},""" +
            """"${TRACTION_MOTOR_1A_POWER.jsonFieldName}" : ${payload.readField(TRACTION_MOTOR_1A_POWER)},""" +
            """"${TRACTION_MOTOR_1B_POWER.jsonFieldName}" : ${payload.readField(TRACTION_MOTOR_1B_POWER)},""" +
            """"${TRACTION_MOTOR_2A_POWER.jsonFieldName}" : ${payload.readField(TRACTION_MOTOR_2A_POWER)},""" +
            """"${TRACTION_MOTOR_2B_POWER.jsonFieldName}" : ${payload.readField(TRACTION_MOTOR_2B_POWER)},""" +
            """"${TRACTION_MOTOR_1A_TEMP.jsonFieldName}" : ${payload.readField(TRACTION_MOTOR_1A_TEMP)},""" +
            """"${TRACTION_MOTOR_1B_TEMP.jsonFieldName}" : ${payload.readField(TRACTION_MOTOR_1B_TEMP)},""" +
            """"${TRACTION_MOTOR_2B_TEMP.jsonFieldName}" : ${payload.readField(TRACTION_MOTOR_2B_TEMP)},""" +
            """"${TRACTION_MOTOR_2A_TEMP.jsonFieldName}" : ${payload.readField(TRACTION_MOTOR_2A_TEMP)},""" +
            """"${POWER_CONVERTER_1A_COOLING_WATER_TEMP.jsonFieldName}" : ${payload.readField(POWER_CONVERTER_1A_COOLING_WATER_TEMP)},""" +
            """"${POWER_CONVERTER_1B_COOLING_WATER_TEMP.jsonFieldName}" : ${payload.readField(POWER_CONVERTER_1B_COOLING_WATER_TEMP)},""" +
            """"${POWER_CONVERTER_2A_COOLING_WATER_TEMP.jsonFieldName}" : ${payload.readField(POWER_CONVERTER_2A_COOLING_WATER_TEMP)},""" +
            """"${POWER_CONVERTER_2B_COOLING_WATER_TEMP.jsonFieldName}" : ${payload.readField(POWER_CONVERTER_2B_COOLING_WATER_TEMP)},""" +
            """"${MAIN_BRAKE_PIPE_PRESSURE.jsonFieldName}" : ${payload.readField(MAIN_BRAKE_PIPE_PRESSURE)},""" +
            """"${BRAKE_CYLINDER_PRESSURE_AT_AXLE.jsonFieldName}" : ${payload.readField(BRAKE_CYLINDER_PRESSURE_AT_AXLE).map { it / 10f }.joinToString(",","[","]")},""" +
            """"${WHEEL_SLIPPAGE_PROTECTION_ACTIVE.jsonFieldName}" : ${payload.readField(WHEEL_SLIPPAGE_PROTECTION_ACTIVE)},""" +
            """"${SLIPPAGE_1A.jsonFieldName}" : ${payload.readField(SLIPPAGE_1A)},""" +
            """"${SLIPPAGE_1B.jsonFieldName}" : ${payload.readField(SLIPPAGE_1B)},""" +
            """"${SLIPPAGE_2A.jsonFieldName}" : ${payload.readField(SLIPPAGE_2A)},""" +
            """"${SLIPPAGE_2B.jsonFieldName}" : ${payload.readField(SLIPPAGE_2B)},""" +
            """"${ENERGY_CONSUMPTION.jsonFieldName}" : ${payload.readField(ENERGY_CONSUMPTION)},""" +
            """"${ENERGY_RECUPERATION.jsonFieldName}" : ${payload.readField(ENERGY_RECUPERATION)},""" +
            """"${CATENARY_VOLTAGE.jsonFieldName}" : ${payload.readField(CATENARY_VOLTAGE) / 10f},""" +
            """"${INSIDE_TEMP_COACH_A.jsonFieldName}" : ${payload.readField(INSIDE_TEMP_COACH_A)},""" +
            """"${INSIDE_TEMP_COACH_B.jsonFieldName}" : ${payload.readField(INSIDE_TEMP_COACH_B)},""" +
            """"${INSIDE_TEMP_COACH_C.jsonFieldName}" : ${payload.readField(INSIDE_TEMP_COACH_C)},""" +
            """"${INSIDE_TEMP_COACH_D.jsonFieldName}" : ${payload.readField(INSIDE_TEMP_COACH_D)},""" +
            """"${TOILET_FRESH_WATER_LEVEL.jsonFieldName}" : ${payload.readField(TOILET_FRESH_WATER_LEVEL)},""" +
            """"${TOILET_WASTE_WATER_LEVEL.jsonFieldName}" : ${payload.readField(TOILET_WASTE_WATER_LEVEL)},""" +
            """"${FLAGS.jsonFieldName}" : ${payload.readField(FLAGS).joinToString(",","[","]")},""" +
            """"${NUMBER_OF_VEHICLES.jsonFieldName}" : ${payload.readField(NUMBER_OF_VEHICLES)},""" +
            """"${VEHICLE_POS_IN_TRAIN.jsonFieldName}" : ${payload.readField(VEHICLE_POS_IN_TRAIN)},""" +
            """"${VEHICLE_NUMBER.jsonFieldName}" : ${payload.readField(VEHICLE_NUMBER).joinToString(",","[","]")},""" +
            """"${ORIENTATION_VEHICLE.jsonFieldName}" : ${payload.readField(ORIENTATION_VEHICLE)},""" +
            """"${EMERGENCY_BRAKE.jsonFieldName}" : ${payload.readField(EMERGENCY_BRAKE)},""" +
            """"${SHOW_IMAGE_OF_CAMERA_N.jsonFieldName}" : ${payload.readField(SHOW_IMAGE_OF_CAMERA_N)},""" +
            """"${SHOW_IMAGE_OF_VEHICLE_N.jsonFieldName}" : ${payload.readField(SHOW_IMAGE_OF_VEHICLE_N)},""" +
            """"${OUTSIDE_TEMP.jsonFieldName}" : ${payload.readField(OUTSIDE_TEMP) / 10f},""" +
            """"${TRAIN_NUMBER.jsonFieldName}" : ${payload.readField(TRAIN_NUMBER)},""" +
            """"${GPS_SPEED.jsonFieldName}" : ${payload.readField(GPS_SPEED)},""" +
            """"${RESERVED6.jsonFieldName}" : ${payload.readField(RESERVED6)},""" +
            """"${GPSX.jsonFieldName}" : ${NMEAToGPS(payload.readField(GPSX).toDouble())},""" +
            """"${GPSY.jsonFieldName}" : ${NMEAToGPS(payload.readField(GPSY).toDouble())},""" +
            """"${TIME.jsonFieldName}" : "${payload.readField(TIME)}"""" +
            "}"
    }

    fun NMEAToGPS(nmea : Double) : Double{
        return floor(nmea / 100) + (nmea - floor(nmea / 100) * 100) / 60
    }

}