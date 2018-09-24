package ru.okabanov.challenge.emulator

import java.util.{Timer, TimerTask}
import java.util.concurrent.{Executors, TimeUnit}

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import ru.okabanov.challenge.{DeviceLocation, DeviceLogData, InputLog, LogKafkaProducer}

class DeviceEmulator(deviceId: String, producer: LogKafkaProducer, topic: String) {
  val mapper = new ObjectMapper() with ScalaObjectMapper
  mapper.registerModule(DefaultScalaModule)

  lazy private val random = scala.util.Random
  lazy private val scheduler = Executors.newScheduledThreadPool(1)

  def start(): Unit = {
    // Random delay 900..1000 milliseconds
    val delay = 900 + random.nextInt(100)
    scheduler.scheduleWithFixedDelay(new Runnable {
      override def run(): Unit = {
        val data = mapper.writeValueAsString(Seq(InputLog(buildEmulatedLog(deviceId))))
        producer.send(data, topic)
      }
    }, 0, delay, TimeUnit.MILLISECONDS)
  }

  override def finalize(): Unit = {
    scheduler.shutdown()
  }


  private def buildEmulatedLog(deviceId: String) = {
    DeviceLogData(
      deviceId = deviceId,
      temperature = random.nextInt(5 + random.nextInt(35)),
      location = DeviceLocation(
        latitude = 52.14691120000001d + random.nextDouble(),
        longitude = 11.658838699999933d + random.nextDouble()
      ),
      time = System.currentTimeMillis() / 1000
    )
  }
}
