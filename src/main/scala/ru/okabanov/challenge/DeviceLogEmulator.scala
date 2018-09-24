package ru.okabanov.challenge

import java.io.FileInputStream
import java.util.Properties
import java.util.concurrent.{Executors, TimeUnit}

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper

/**
  * @author okabanov
  */
object DeviceLogEmulator {
  lazy private val random = scala.util.Random

  val mapper = new ObjectMapper() with ScalaObjectMapper
  mapper.registerModule(DefaultScalaModule)

  def main(args: Array[String]): Unit = {

    val (brokers, topic) =
      try {
        val prop = new Properties()
        prop.load(new FileInputStream("spark_application.conf"))
        (
          prop.getProperty("spark.iot-log-parser.kafka.brokers"),
          prop.getProperty("spark.iot-log-parser.kafka.input-topic")
        )
      } catch { case e: Exception =>
        e.printStackTrace()
        sys.exit(1)
      }

    val producer = new LogKafkaProducer(brokers)

    val sheduler = Executors.newScheduledThreadPool(2)
    sheduler.scheduleWithFixedDelay(new Runnable {
      override def run(): Unit = {
        val baseUid = "11c1310e-c0c2-461b-a4eb-f6bf8da2d23c-"
        Seq(
          InputLog(buildEmulatedLog(baseUid + "1")),
          InputLog(buildEmulatedLog(baseUid + "2")),
          InputLog(buildEmulatedLog(baseUid + "3"))
        ).foreach { logRow =>
          val data = mapper.writeValueAsString(Seq(logRow))
          producer.send(data, topic)
        }
      }
    }, 0, 1, TimeUnit.SECONDS)

  }

  private def buildEmulatedLog(deviceId: String) = {
    DeviceLogData(
      deviceId = deviceId,
      temperature = random.nextInt(40),
      location = DeviceLocation(
        latitude = 52.14691120000001d + random.nextDouble(),
        longitude = 11.658838699999933d + random.nextDouble()
      ),
      time = System.currentTimeMillis() / 1000
    )
  }
}