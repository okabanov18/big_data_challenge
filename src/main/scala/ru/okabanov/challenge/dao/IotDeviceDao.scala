package ru.okabanov.challenge.dao

import ru.okabanov.challenge.DeviceLogData

trait IotDeviceDao {
  def save(data: DeviceLogData)
  def init()
  def close()
}
