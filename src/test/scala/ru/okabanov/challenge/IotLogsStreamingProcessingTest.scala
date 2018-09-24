package ru.okabanov.challenge

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.scalatest.FlatSpec

import scala.collection.mutable

class IotLogsStreamingProcessingTest extends FlatSpec {

  val sparkConf = new SparkConf()
    .setMaster("local[*]")
    .setAppName("spark-streaming-testing")

  val ssc = new StreamingContext(sparkConf, Seconds(1))

  val mapper = new ObjectMapper() with ScalaObjectMapper
  mapper.registerModule(DefaultScalaModule)

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

    ssc.start()

    rddQueue += ssc.sparkContext.parallelize(List(
      ("key", mapper.writeValueAsString(Seq(InputLog(deviceLogData))))
    ))


  }





}
