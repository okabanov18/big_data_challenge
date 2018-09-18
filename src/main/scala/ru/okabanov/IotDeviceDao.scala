package ru.okabanov

import java.sql.Timestamp
import java.text.{DateFormat, SimpleDateFormat}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.{HBaseAdmin, HTable, Put}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, HColumnDescriptor, HTableDescriptor}

class IotDeviceDao {

  val formatTime = new ThreadLocal[DateFormat] () {
    override def initialValue(): DateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssXXX")
  }

  private val cfDevice = Bytes.toBytes("device")
  private val cfMetric = Bytes.toBytes("metric")
  private val cfTime   = Bytes.toBytes("time")
  private val cfLocation = Bytes.toBytes("location")

  private var hbaseConf: Configuration = _
  private var hTable: HTable = _

  def save(data: DeviceLogData) {
    val transformedTime = formatTime.get().format(new Timestamp(data.time))

    val put = new Put(Bytes.toBytes("key" + System.currentTimeMillis()))
    put.add(cfDevice, Bytes.toBytes("id"), Bytes.toBytes(data.deviceId.toString))
    put.add(cfMetric, Bytes.toBytes("temperature"), Bytes.toBytes(data.temperature.toString))
    put.add(cfLocation, Bytes.toBytes("latitude"), Bytes.toBytes(data.location.latitude.toString))
    put.add(cfLocation, Bytes.toBytes("longitude"), Bytes.toBytes(data.location.longitude.toString))
    put.add(cfTime, Bytes.toBytes("time"), Bytes.toBytes(transformedTime))

    hTable.put(put)
  }

  def init(): Unit = {
    hbaseConf = HBaseConfiguration.create()
    val tableName = "iot_device_log"
    hbaseConf.set("hbase.mapred.outputtable", tableName)
    hbaseConf.set("hbase.zookeeper.quorum","quickstart.cloudera")
    hbaseConf.set("hbase.zookeeper.property.client.port","2181")
    val admin = new HBaseAdmin(hbaseConf)
    hTable = new HTable(hbaseConf,tableName)
    createIfNotExist(tableName, admin)
  }

  def close(): Unit = {
    hTable.close()
  }

  private def createIfNotExist(tableName: String, admin: HBaseAdmin) = {
    if (!admin.isTableAvailable(tableName)) {
      val tableDesc = new HTableDescriptor(tableName)
      tableDesc.addFamily(new HColumnDescriptor(cfDevice))
      tableDesc.addFamily(new HColumnDescriptor(cfMetric))
      tableDesc.addFamily(new HColumnDescriptor(cfLocation))
      tableDesc.addFamily(new HColumnDescriptor(cfTime))
    }
  }
}
