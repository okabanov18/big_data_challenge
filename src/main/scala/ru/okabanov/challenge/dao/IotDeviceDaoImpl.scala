package ru.okabanov.challenge.dao

import java.sql.Timestamp
import java.text.{DateFormat, SimpleDateFormat}

import org.apache.hadoop.hbase.client.{HBaseAdmin, HTable, Put}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, HColumnDescriptor, HTableDescriptor}
import ru.okabanov.challenge.model.DeviceLogData
import scala.collection.JavaConverters._

/**
  * @author okabanov
  */
class IotDeviceDaoImpl extends IotDeviceDao {

  val formatTime = new ThreadLocal[DateFormat]() {
    override def initialValue(): DateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssXXX")
  }

  private val cfDevice   = Bytes.toBytes("device")
  private val cfMetric   = Bytes.toBytes("metric")
  private val cfTime     = Bytes.toBytes("time")
  private val cfLocation = Bytes.toBytes("location")

  private val colId          = Bytes.toBytes("id")
  private val colTemperature = Bytes.toBytes("temperature")
  private val colLatitude    = Bytes.toBytes("latitude")
  private val colLongitude   = Bytes.toBytes("longitude")
  private val colTime        = Bytes.toBytes("time")

  override def saveBatch(data: Seq[DeviceLogData]): Unit = {
    val hTable = init()
    try {
      val puts = data.map { logRow =>
        val transformedTime = formatTime.get().format(new Timestamp(logRow.time * 1000))

        val put = new Put(Bytes.toBytes(s"${logRow.deviceId.toString}_${System.currentTimeMillis()}"))
        put.add(cfDevice, colId, Bytes.toBytes(logRow.deviceId.toString))
        put.add(cfMetric, colTemperature, Bytes.toBytes(logRow.temperature.toString))
        put.add(cfLocation, colLatitude, Bytes.toBytes(logRow.location.latitude.toString))
        put.add(cfLocation, colLongitude, Bytes.toBytes(logRow.location.longitude.toString))
        put.add(cfTime, colTime, Bytes.toBytes(transformedTime))

        put
      }.asJava

      hTable.put(puts)
    } finally {
      hTable.close()
    }
  }

  private def init(): HTable = {
    val hbaseConf = HBaseConfiguration.create()
    val tableName = "iot_device_log"
    hbaseConf.set("hbase.mapred.outputtable", tableName)
    hbaseConf.set("hbase.zookeeper.quorum", "quickstart.cloudera")
    hbaseConf.set("hbase.zookeeper.property.client.port", "2181")
    val admin = new HBaseAdmin(hbaseConf)
    val hTable = new HTable(hbaseConf, tableName)
    createIfNotExist(tableName, admin)

    hTable
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
