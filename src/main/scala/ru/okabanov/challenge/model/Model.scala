package ru.okabanov.challenge.model

case class DeviceLogData(
                          deviceId: String,
                          temperature: Int,
                          location: DeviceLocation,
                          time: Long
                        )

case class DeviceLocation(
                           latitude: Double,
                           longitude: Double
                         )

case class InputLog(data: DeviceLogData)
