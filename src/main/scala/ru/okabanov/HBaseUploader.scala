package ru.okabanov

import org.apache.hadoop.hbase.client.{HBaseAdmin, HTable, Put}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, HColumnDescriptor, HTableDescriptor}

object HBaseUploader {
  def save(data: DeviceLogData) {
    val hbaseConf = HBaseConfiguration.create()
    val tableName = "iot_device_log"
    hbaseConf.set("hbase.mapred.outputtable", tableName)
    hbaseConf.set("hbase.zookeeper.quorum","quickstart.cloudera")
    hbaseConf.set("hbase.zookeeper.property.client.port","2181")
    val admin = new HBaseAdmin(hbaseConf)
    val cfDevice = Bytes.toBytes("device")
    val cfMetric = Bytes.toBytes("metric")
    val cfTime   = Bytes.toBytes("time")
    val cfLocation = Bytes.toBytes("location")

    createIfNotExist(tableName, admin, cfDevice, cfMetric, cfLocation, cfTime)

    val put = new Put(Bytes.toBytes(System.currentTimeMillis()))
    put.add(cfDevice, Bytes.toBytes("id"), Bytes.toBytes(data.deviceId))
    put.add(cfMetric, Bytes.toBytes("temperature"), Bytes.toBytes(data.temperature))
    put.add(cfLocation, Bytes.toBytes("latitude"), Bytes.toBytes(data.location.latitude))
    put.add(cfLocation, Bytes.toBytes("longitude"), Bytes.toBytes(data.location.longitude))
    put.add(cfTime, Bytes.toBytes("time"), Bytes.toBytes(data.time))

    val hTable = new HTable(hbaseConf,tableName)
    hTable.put(put)
    hTable.close()
  }

  private def createIfNotExist(tableName: String, admin: HBaseAdmin, cfDevice: Array[Byte], cfMetric: Array[Byte], cfLocation: Array[Byte], cfTime: Array[Byte]) = {
    if (!admin.isTableAvailable(tableName)) {
      val tableDesc = new HTableDescriptor(tableName)
      tableDesc.addFamily(new HColumnDescriptor(cfDevice))
      tableDesc.addFamily(new HColumnDescriptor(cfMetric))
      tableDesc.addFamily(new HColumnDescriptor(cfLocation))
      tableDesc.addFamily(new HColumnDescriptor(cfTime))
    }
  }
}
