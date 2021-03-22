package fi.hsl.transitdata.eke_sink.messages.stadlerUDP


import fi.hsl.transitdata.eke_sink.converters.*
import fi.hsl.transitdata.eke_sink.messages.mqtt_header.*
import kotlin.math.floor

object StadlerUDPParser {

    //val TIMESTAMP = FieldDefinition(TIMESTAMP, TIMESTAMP_SIZE, TO_DATE, "timestamp") Timestamp only in the test file
    val INDEX = FieldDefinition(NTP_HUNDRED_OF_SECONDS, 1, TO_UNSIGNED_INT, "index")
    val RESERVED1 = FieldDefinition(INDEX, 1, TO_UNSIGNED_INT, "reserved1")
    val VEHICLE_SHUTTING_DOWN = FieldDefinition(
        RESERVED1, 1,
        TO_UNSIGNED_INT,
        "vehicleShuttingDown"
    )
    val RESERVED2 = FieldDefinition(VEHICLE_SHUTTING_DOWN, 1, TO_UNSIGNED_INT, "reserved2")
    val SPEED = FieldDefinition(RESERVED2, 4, TO_FLOAT, "speed")
    val ODOMETER = FieldDefinition(SPEED, 2, TO_UNSIGNED_INT, "odometer")
    val RESERVED3 = FieldDefinition(ODOMETER, 2, TO_UNSIGNED_INT, "reserved3")
    val NUMBER_OF_KILOMETERS = FieldDefinition(
        RESERVED3, 4,
        TO_UNSIGNED_INT,
        "numberOfKilometers"
    )
    val ACCELERATION = FieldDefinition(NUMBER_OF_KILOMETERS, 4, TO_FLOAT, "acceleration")
    val STANDSTILL = FieldDefinition(ACCELERATION, 1, TO_UNSIGNED_INT, "standstill")
    val DOORS_L_OPEN_VEHICLE1 = FieldDefinition(
        STANDSTILL, 1,
        TO_UNSIGNED_INT,
        "doorsLOpenVehicle1"
    )
    val DOORS_R_OPEN_VEHICLE1 = FieldDefinition(
        DOORS_L_OPEN_VEHICLE1, 1,
        TO_UNSIGNED_INT,
        "doorsROpenVehicle1"
    )
    val DOORS_L_OPEN_VEHICLE2 = FieldDefinition(
        DOORS_R_OPEN_VEHICLE1, 1,
        TO_UNSIGNED_INT,
        "doorsLOpenVehicle2"
    )
    val DOORS_R_OPEN_VEHICLE2 = FieldDefinition(
        DOORS_L_OPEN_VEHICLE2, 1,
        TO_UNSIGNED_INT,
        "doorsROpenVehicle2"
    )
    val DOORS_L_OPEN_VEHICLE3 = FieldDefinition(
        DOORS_R_OPEN_VEHICLE2, 1,
        TO_UNSIGNED_INT,
        "doorsLOpenVehicle3"
    )
    val DOORS_R_OPEN_VEHICLE3 = FieldDefinition(
        DOORS_L_OPEN_VEHICLE3, 1,
        TO_UNSIGNED_INT,
        "doorsROpenVehicle3"
    )
    val DOORS_L_OPEN_VEHICLE4 = FieldDefinition(
        DOORS_R_OPEN_VEHICLE3, 1,
        TO_UNSIGNED_INT,
        "doorsLOpenVehicle4"
    )
    val DOORS_R_OPEN_VEHICLE4 = FieldDefinition(
        DOORS_L_OPEN_VEHICLE4, 1,
        TO_UNSIGNED_INT,
        "doorsROpenVehicle4"
    )
    val ACTIVE_SETPOINT_SPEED = FieldDefinition(
        DOORS_R_OPEN_VEHICLE4, 1,
        TO_UNSIGNED_INT,
        "activeSetpointSpeed"
    )
    val RESERVED4 = FieldDefinition(ACTIVE_SETPOINT_SPEED, 1, TO_UNSIGNED_INT, "reserved4")
    val RESERVED5 = FieldDefinition(RESERVED4, 1, TO_UNSIGNED_INT, "reserved5")
    val TRACTION_LEVER_POSITION = FieldDefinition(
        RESERVED5, 4,
        TO_FLOAT,
        "tractionLeverPosition"
    )
    val TRACTION_SET_POINT =
        FieldDefinition(TRACTION_LEVER_POSITION, 4, TO_FLOAT, "tractionSetPoint")
    val TRACTION_ACTUAL = FieldDefinition(TRACTION_SET_POINT, 4, TO_FLOAT, "tractionActual")
    val TRACTION_MOTOR_1A_POWER = FieldDefinition(
        TRACTION_ACTUAL, 4,
        TO_FLOAT,
        "tractionMotor1APower"
    )
    val TRACTION_MOTOR_1B_POWER = FieldDefinition(
        TRACTION_MOTOR_1A_POWER, 4,
        TO_FLOAT,
        "tractionMotor1BPower"
    )
    val TRACTION_MOTOR_2A_POWER = FieldDefinition(
        TRACTION_MOTOR_1B_POWER, 4,
        TO_FLOAT,
        "tractionMotor2APower"
    )
    val TRACTION_MOTOR_2B_POWER = FieldDefinition(
        TRACTION_MOTOR_2A_POWER, 4,
        TO_FLOAT,
        "tractionMotor2BPower"
    )
    val TRACTION_MOTOR_1A_TEMP =
        FieldDefinition(TRACTION_MOTOR_2B_POWER, 4, TO_FLOAT, "tractionMotor1ATemp")
    val TRACTION_MOTOR_1B_TEMP =
        FieldDefinition(TRACTION_MOTOR_1A_TEMP, 4, TO_FLOAT, "tractionMotor1BTemp")
    val TRACTION_MOTOR_2A_TEMP =
        FieldDefinition(TRACTION_MOTOR_1B_TEMP, 4, TO_FLOAT, "tractionMotor2ATemp")
    val TRACTION_MOTOR_2B_TEMP =
        FieldDefinition(TRACTION_MOTOR_2A_TEMP, 4, TO_FLOAT, "tractionMotor2BTemp")
    val POWER_CONVERTER_1A_COOLING_WATER_TEMP = FieldDefinition(
        TRACTION_MOTOR_2B_TEMP, 4,
        TO_FLOAT,
        "powerConverter1ACoolingWaterTemp"
    )
    val POWER_CONVERTER_1B_COOLING_WATER_TEMP = FieldDefinition(
        POWER_CONVERTER_1A_COOLING_WATER_TEMP, 4,
        TO_FLOAT,
        "powerConverter1BCoolingWaterTemp"
    )
    val POWER_CONVERTER_2A_COOLING_WATER_TEMP = FieldDefinition(
        POWER_CONVERTER_1B_COOLING_WATER_TEMP, 4,
        TO_FLOAT,
        "powerConverter2ACoolingWaterTemp"
    )
    val POWER_CONVERTER_2B_COOLING_WATER_TEMP = FieldDefinition(
        POWER_CONVERTER_2A_COOLING_WATER_TEMP,4,
        TO_FLOAT,
        "powerConverter2BCoolingWaterTemp"
    )
    val MAIN_BRAKE_PIPE_PRESSURE = FieldDefinition(
        POWER_CONVERTER_2B_COOLING_WATER_TEMP, 4,
        TO_FLOAT,
        "mainBrakePipePressure"
    )
    val BRAKE_CYLINDER_PRESSURE_AT_AXLE = FieldDefinition(
        MAIN_BRAKE_PIPE_PRESSURE, 10,
        TO_UNSIGNED_INT_ARRAY,
        "breakCylinderPressureAtAxle",
        StringFormatter { value -> value.map { it / 10f }.joinToString(",", "[", "]") })
    val WHEEL_SLIPPAGE_PROTECTION_ACTIVE = FieldDefinition(
        BRAKE_CYLINDER_PRESSURE_AT_AXLE, 2,
        TO_UNSIGNED_INT,
        "wheelSlippageProtectionActive"
    )
    val SLIPPAGE_1A = FieldDefinition(WHEEL_SLIPPAGE_PROTECTION_ACTIVE, 4, TO_FLOAT, "slippage1A")
    val SLIPPAGE_1B = FieldDefinition(SLIPPAGE_1A, 4, TO_FLOAT, "slippage1B")
    val SLIPPAGE_2A = FieldDefinition(SLIPPAGE_1B, 4, TO_FLOAT, "slippage2A")
    val SLIPPAGE_2B = FieldDefinition(SLIPPAGE_2A, 4, TO_FLOAT, "slippage2B")
    val ENERGY_CONSUMPTION =
        FieldDefinition(SLIPPAGE_2B, 4, TO_UNSIGNED_INT, "energyConsumption")
    val ENERGY_RECUPERATION =
        FieldDefinition(ENERGY_CONSUMPTION, 4, TO_UNSIGNED_INT, "energyRecuperation")
    val CATENARY_VOLTAGE = FieldDefinition(
        ENERGY_RECUPERATION, 2,
        TO_UNSIGNED_INT,
        "catenaryVoltage",
        StringFormatter { value -> (value / 10f).toString() })
    val INSIDE_TEMP_COACH_A =
        FieldDefinition(CATENARY_VOLTAGE, 1, TO_UNSIGNED_INT, "insideTempCoachA")
    val INSIDE_TEMP_COACH_B =
        FieldDefinition(INSIDE_TEMP_COACH_A, 1, TO_UNSIGNED_INT, "insideTempCoachB")
    val INSIDE_TEMP_COACH_C =
        FieldDefinition(INSIDE_TEMP_COACH_B, 1, TO_UNSIGNED_INT, "insideTempCoachC")
    val INSIDE_TEMP_COACH_D =
        FieldDefinition(INSIDE_TEMP_COACH_C, 1, TO_UNSIGNED_INT, "insideTempCoachD")
    val TOILET_FRESH_WATER_LEVEL = FieldDefinition(
        INSIDE_TEMP_COACH_D, 1,
        TO_UNSIGNED_INT,
        "toiletFreshWaterLevel"
    )
    val TOILET_WASTE_WATER_LEVEL = FieldDefinition(
        TOILET_FRESH_WATER_LEVEL, 1,
        TO_UNSIGNED_INT,
        "toiletWasteWaterLevel"
    )
    val FLAGS = FieldDefinition(TOILET_WASTE_WATER_LEVEL, 4, TO_UNSIGNED_INT_ARRAY, "flags",
        { value -> value.joinToString(",", "[", "]") })
    val NUMBER_OF_VEHICLES =
        FieldDefinition(FLAGS, 1, TO_UNSIGNED_INT, "numberOfVehicles")
    val VEHICLE_POS_IN_TRAIN = FieldDefinition(
        NUMBER_OF_VEHICLES, 1,
        TO_UNSIGNED_INT,
        "vehiclePosInTrain"
    )
    val VEHICLE_NUMBER =
        FieldDefinition(VEHICLE_POS_IN_TRAIN, 4, TO_UNSIGNED_INT_ARRAY, "vehicleNumber",
            { value -> value.joinToString(",", "[", "]") })
    val ORIENTATION_VEHICLE =
        FieldDefinition(VEHICLE_NUMBER, 1, TO_UNSIGNED_INT, "orientationVehicle")
    val EMERGENCY_BRAKE =
        FieldDefinition( ORIENTATION_VEHICLE, 1, TO_UNSIGNED_INT, "emergencyBrake")
    val SHOW_IMAGE_OF_CAMERA_N = FieldDefinition(
        EMERGENCY_BRAKE, 1,
        TO_UNSIGNED_INT,
        "showImageOfCameraN"
    )
    val SHOW_IMAGE_OF_VEHICLE_N = FieldDefinition(
        SHOW_IMAGE_OF_CAMERA_N, 1,
        TO_UNSIGNED_INT,
        "showImageOfVehicleN"
    )
    val OUTSIDE_TEMP = FieldDefinition(
        SHOW_IMAGE_OF_VEHICLE_N, 2,
        TO_INT,
        "outsideTemp",
        { value -> (value / 10f).toString() })
    val TRAIN_NUMBER = FieldDefinition(OUTSIDE_TEMP, 2, TO_UNSIGNED_INT, "trainNumber")
    val GPS_SPEED = FieldDefinition(TRAIN_NUMBER, 1, TO_UNSIGNED_INT, "GPSSpeed")
    val RESERVED6 = FieldDefinition(GPS_SPEED, 1, TO_UNSIGNED_INT, "reserved6")
    val GPSX = FieldDefinition(
        RESERVED6, 4,
        TO_FLOAT,
        "GPSX",
        { value -> (NMEAToGPS(value.toDouble()).toString()) })
    val GPSY = FieldDefinition(
        GPSX, 4,
        TO_FLOAT,
        "GPSY",
        { value -> (NMEAToGPS(value.toDouble()).toString()) })
    //The timestamp of the GPS time is based on the EET timezone. This is not standard so we should substract the difference between UTC and EET timezones
    val TIME = FieldDefinition(GPSY, 4, TO_DATE_FROM_EET, "GPSTime")
    val STADLER_UDP_SIZE = TIME.offset + TIME.size

    val fields =  arrayOf(*headerFields, INDEX,
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


    fun toJson(payload : ByteArray, topic : String) : String {
        return "{" +
            """"${MQTT_HEADER_1ST_PART.fieldName}" : ${payload.readField(MQTT_HEADER_1ST_PART)},""" +
            """"${EKE_TIME.fieldName}" : "${payload.readField(EKE_TIME)}",""" +
            """"${EKE_HUNDRED_OF_SECONDS.fieldName}" : ${payload.readField(EKE_HUNDRED_OF_SECONDS)},""" +
            """"${NTP_TIME.fieldName}" : "${payload.readField(NTP_TIME)}",""" +
            """"${NTP_HUNDRED_OF_SECONDS.fieldName}" : ${payload.readField(NTP_HUNDRED_OF_SECONDS)},""" +
            //""""${TIMESTAMP.jsonFieldName}" : "${payload.readField(TIMESTAMP)}",""" +
            """"${INDEX.fieldName}" : ${payload.readField(INDEX)},""" +
            """"${RESERVED1.fieldName}" : ${payload.readField(RESERVED1)},""" +
            """"${VEHICLE_SHUTTING_DOWN.fieldName}" : ${payload.readField(VEHICLE_SHUTTING_DOWN)},""" +
            """"${RESERVED2.fieldName}" : ${payload.readField(RESERVED2)},""" +
            """"${SPEED.fieldName}" : ${payload.readField(SPEED)},""" +
            """"${ODOMETER.fieldName}" : ${payload.readField(ODOMETER)},""" +
            """"${RESERVED3.fieldName}" : ${payload.readField(RESERVED3)},""" +
            """"${NUMBER_OF_KILOMETERS.fieldName}" : ${payload.readField(NUMBER_OF_KILOMETERS)},""" +
            """"${ACCELERATION.fieldName}" : ${payload.readField(ACCELERATION)},""" +
            """"${STANDSTILL.fieldName}" : ${payload.readField(STANDSTILL)},""" +
            """"${DOORS_L_OPEN_VEHICLE1.fieldName}" : ${payload.readField(DOORS_L_OPEN_VEHICLE1)},""" +
            """"${DOORS_R_OPEN_VEHICLE1.fieldName}" : ${payload.readField(DOORS_R_OPEN_VEHICLE1)},""" +
            """"${DOORS_L_OPEN_VEHICLE2.fieldName}" : ${payload.readField(DOORS_L_OPEN_VEHICLE2)},""" +
            """"${DOORS_R_OPEN_VEHICLE2.fieldName}" : ${payload.readField(DOORS_R_OPEN_VEHICLE2)},""" +
            """"${DOORS_L_OPEN_VEHICLE3.fieldName}" : ${payload.readField(DOORS_L_OPEN_VEHICLE3)},""" +
            """"${DOORS_R_OPEN_VEHICLE3.fieldName}" : ${payload.readField(DOORS_R_OPEN_VEHICLE3)},""" +
            """"${DOORS_L_OPEN_VEHICLE4.fieldName}" : ${payload.readField(DOORS_L_OPEN_VEHICLE4)},""" +
            """"${DOORS_R_OPEN_VEHICLE4.fieldName}" : ${payload.readField(DOORS_R_OPEN_VEHICLE4)},""" +
            """"${ACTIVE_SETPOINT_SPEED.fieldName}" : ${payload.readField(ACTIVE_SETPOINT_SPEED)},""" +
            """"${RESERVED4.fieldName}" : ${payload.readField(RESERVED4)},""" +
            """"${RESERVED5.fieldName}" : ${payload.readField(RESERVED5)},""" +
            """"${TRACTION_LEVER_POSITION.fieldName}" : ${payload.readField(TRACTION_LEVER_POSITION)},""" +
            """"${TRACTION_SET_POINT.fieldName}" : ${payload.readField(TRACTION_SET_POINT)},""" +
            """"${TRACTION_ACTUAL.fieldName}" : ${payload.readField(TRACTION_ACTUAL)},""" +
            """"${TRACTION_MOTOR_1A_POWER.fieldName}" : ${payload.readField(TRACTION_MOTOR_1A_POWER)},""" +
            """"${TRACTION_MOTOR_1B_POWER.fieldName}" : ${payload.readField(TRACTION_MOTOR_1B_POWER)},""" +
            """"${TRACTION_MOTOR_2A_POWER.fieldName}" : ${payload.readField(TRACTION_MOTOR_2A_POWER)},""" +
            """"${TRACTION_MOTOR_2B_POWER.fieldName}" : ${payload.readField(TRACTION_MOTOR_2B_POWER)},""" +
            """"${TRACTION_MOTOR_1A_TEMP.fieldName}" : ${payload.readField(TRACTION_MOTOR_1A_TEMP)},""" +
            """"${TRACTION_MOTOR_1B_TEMP.fieldName}" : ${payload.readField(TRACTION_MOTOR_1B_TEMP)},""" +
            """"${TRACTION_MOTOR_2B_TEMP.fieldName}" : ${payload.readField(TRACTION_MOTOR_2B_TEMP)},""" +
            """"${TRACTION_MOTOR_2A_TEMP.fieldName}" : ${payload.readField(TRACTION_MOTOR_2A_TEMP)},""" +
            """"${POWER_CONVERTER_1A_COOLING_WATER_TEMP.fieldName}" : ${payload.readField(
                POWER_CONVERTER_1A_COOLING_WATER_TEMP
            )},""" +
            """"${POWER_CONVERTER_1B_COOLING_WATER_TEMP.fieldName}" : ${payload.readField(
                POWER_CONVERTER_1B_COOLING_WATER_TEMP
            )},""" +
            """"${POWER_CONVERTER_2A_COOLING_WATER_TEMP.fieldName}" : ${payload.readField(
                POWER_CONVERTER_2A_COOLING_WATER_TEMP
            )},""" +
            """"${POWER_CONVERTER_2B_COOLING_WATER_TEMP.fieldName}" : ${payload.readField(
                POWER_CONVERTER_2B_COOLING_WATER_TEMP
            )},""" +
            """"${MAIN_BRAKE_PIPE_PRESSURE.fieldName}" : ${payload.readField(MAIN_BRAKE_PIPE_PRESSURE)},""" +
            """"${BRAKE_CYLINDER_PRESSURE_AT_AXLE.fieldName}" : ${payload.readField(BRAKE_CYLINDER_PRESSURE_AT_AXLE).map { it / 10f }.joinToString(",","[","]")},""" +
            """"${WHEEL_SLIPPAGE_PROTECTION_ACTIVE.fieldName}" : ${payload.readField(
                WHEEL_SLIPPAGE_PROTECTION_ACTIVE
            )},""" +
            """"${SLIPPAGE_1A.fieldName}" : ${payload.readField(SLIPPAGE_1A)},""" +
            """"${SLIPPAGE_1B.fieldName}" : ${payload.readField(SLIPPAGE_1B)},""" +
            """"${SLIPPAGE_2A.fieldName}" : ${payload.readField(SLIPPAGE_2A)},""" +
            """"${SLIPPAGE_2B.fieldName}" : ${payload.readField(SLIPPAGE_2B)},""" +
            """"${ENERGY_CONSUMPTION.fieldName}" : ${payload.readField(ENERGY_CONSUMPTION)},""" +
            """"${ENERGY_RECUPERATION.fieldName}" : ${payload.readField(ENERGY_RECUPERATION)},""" +
            """"${CATENARY_VOLTAGE.fieldName}" : ${payload.readField(CATENARY_VOLTAGE) / 10f},""" +
            """"${INSIDE_TEMP_COACH_A.fieldName}" : ${payload.readField(INSIDE_TEMP_COACH_A)},""" +
            """"${INSIDE_TEMP_COACH_B.fieldName}" : ${payload.readField(INSIDE_TEMP_COACH_B)},""" +
            """"${INSIDE_TEMP_COACH_C.fieldName}" : ${payload.readField(INSIDE_TEMP_COACH_C)},""" +
            """"${INSIDE_TEMP_COACH_D.fieldName}" : ${payload.readField(INSIDE_TEMP_COACH_D)},""" +
            """"${TOILET_FRESH_WATER_LEVEL.fieldName}" : ${payload.readField(TOILET_FRESH_WATER_LEVEL)},""" +
            """"${TOILET_WASTE_WATER_LEVEL.fieldName}" : ${payload.readField(TOILET_WASTE_WATER_LEVEL)},""" +
            """"${FLAGS.fieldName}" : ${payload.readField(FLAGS).joinToString(",","[","]")},""" +
            """"${NUMBER_OF_VEHICLES.fieldName}" : ${payload.readField(NUMBER_OF_VEHICLES)},""" +
            """"${VEHICLE_POS_IN_TRAIN.fieldName}" : ${payload.readField(VEHICLE_POS_IN_TRAIN)},""" +
            """"${VEHICLE_NUMBER.fieldName}" : ${payload.readField(VEHICLE_NUMBER).joinToString(",","[","]")},""" +
            """"${ORIENTATION_VEHICLE.fieldName}" : ${payload.readField(ORIENTATION_VEHICLE)},""" +
            """"${EMERGENCY_BRAKE.fieldName}" : ${payload.readField(EMERGENCY_BRAKE)},""" +
            """"${SHOW_IMAGE_OF_CAMERA_N.fieldName}" : ${payload.readField(SHOW_IMAGE_OF_CAMERA_N)},""" +
            """"${SHOW_IMAGE_OF_VEHICLE_N.fieldName}" : ${payload.readField(SHOW_IMAGE_OF_VEHICLE_N)},""" +
            """"${OUTSIDE_TEMP.fieldName}" : ${payload.readField(OUTSIDE_TEMP) / 10f},""" +
            """"${TRAIN_NUMBER.fieldName}" : ${payload.readField(TRAIN_NUMBER)},""" +
            """"${GPS_SPEED.fieldName}" : ${payload.readField(GPS_SPEED)},""" +
            """"${RESERVED6.fieldName}" : ${payload.readField(RESERVED6)},""" +
            """"${GPSX.fieldName}" : ${NMEAToGPS(payload.readField(GPSX).toDouble())},""" +
            """"${GPSY.fieldName}" : ${NMEAToGPS(payload.readField(GPSY).toDouble())},""" +
            """"${TIME.fieldName}" : "${payload.readField(TIME)}"""" +
            """"topic" : "$topic"""" +
            "}"
    }

    fun NMEAToGPS(nmea : Double) : Double{
        return floor(nmea / 100) + (nmea - floor(nmea / 100) * 100) / 60
    }

}