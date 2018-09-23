package ru.okabanov.challenge

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
