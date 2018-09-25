package ru.okabanov.challenge.dao

import ru.okabanov.challenge.model.DeviceLogData

trait IotDeviceDao {
  def saveBatch(data: Seq[DeviceLogData])
}
