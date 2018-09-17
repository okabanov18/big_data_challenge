package ru.okabanov

import java.sql.Timestamp
import java.util.concurrent.{Executors, TimeUnit}

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import org.json4s.jackson.JsonMethods

object DeviceLogEmulator {
  lazy val random = scala.util.Random

  def main(args: Array[String]): Unit = {

    val brokers = "quickstart.cloudera:9092"
    val topic = "iot-device-log"
    val producer = new LogKafkaProducer(brokers)

    val sheduler = Executors.newScheduledThreadPool(2)
    sheduler.scheduleWithFixedDelay(new Runnable {
      override def run(): Unit = {
        val baseUid = "11c1310e-c0c2-461b-a4eb-f6bf8da2d23c-"
        val inputs = Seq(
          InputLog(buildEmulatedLog(baseUid + "1")),
          InputLog(buildEmulatedLog(baseUid + "2")),
          InputLog(buildEmulatedLog(baseUid + "3"))
        )
        println("Try to send")
        val data = Serializer.write(inputs)
        producer.send(data, topic)
        println("Sent")
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

object Serializer {
  val mapper = new ObjectMapper() with ScalaObjectMapper
  mapper.registerModule(DefaultScalaModule)

  def write(data: AnyRef) = mapper.writeValueAsString(data)
}

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
