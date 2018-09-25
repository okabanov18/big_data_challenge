package ru.okabanov.challenge.dao

import ru.okabanov.challenge.model.DeviceLogData

trait IotDeviceDao {
  def save(data: DeviceLogData)
  def init()
  def close()
}
