package ru.okabanov.challenge

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Milliseconds, Seconds, StreamingContext}
import org.scalatest.FlatSpec
import ru.okabanov.challenge.model._
import ru.okabanov.challenge.utils.SimpleScalaObjectMapper

import scala.collection.mutable

class LogsStreamingAppTest extends FlatSpec {

  val sparkConf = new SparkConf()
    .setMaster("local[*]")
    .setAppName("spark-streaming-testing")

  val ssc = new StreamingContext(sparkConf, Milliseconds(1))

  behavior of "Log stream processing"

  it should "parse input stream" in {
    val rddQueue = new mutable.Queue[RDD[(String, String)]]()

    val deviceLogData = DeviceLogData(
      deviceId = "12345",
      temperature = 26,
      location = DeviceLocation(
        latitude = 16,
        longitude = 11
      ),
      time = 123456789
    )

    IotStreamingApp.parseInputLogs(ssc.queueStream(rddQueue)).foreachRDD { partition => partition.foreach { data => data} }

    ssc.start()

    rddQueue += ssc.sparkContext.parallelize(
      List(("key", SimpleScalaObjectMapper.writeValueAsString(Seq(InputLog(deviceLogData)))))
    )

    ssc.awaitTerminationOrTimeout(5)
  }
}
