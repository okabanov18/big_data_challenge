package ru.okabanov.challenge.emulator

import java.util.concurrent.{Executors, TimeUnit}

import ru.okabanov.challenge.model.{DeviceLocation, DeviceLogData, InputLog}
import ru.okabanov.challenge.utils.SimpleScalaObjectMapper

class DeviceEmulator(deviceId: String, producer: LogKafkaProducer, topic: String) {

  private lazy val random = scala.util.Random
  private val scheduler = Executors.newScheduledThreadPool(1)

  def start(): Unit = {
    val task = new Runnable() {
      def run() = sendLog()
    }
    scheduler.scheduleAtFixedRate(task, 0, random.nextInt(100) + 900, TimeUnit.MILLISECONDS)
  }

  private def sendLog(): Unit = {
    val data = SimpleScalaObjectMapper.writeValueAsString(Seq(
      InputLog(
        data = buildEmulatedLog(deviceId))
    ))
    producer.send(data, topic)
  }

  override def finalize(): Unit = {
    scheduler.shutdown()
  }


  private def buildEmulatedLog(deviceId: String) = {
    DeviceLogData(
      deviceId = deviceId,
      temperature = random.nextInt(5 + random.nextInt(35)),
      location = DeviceLocation(
        latitude = 52.14691d + random.nextDouble(),
        longitude = 11.65883d + random.nextDouble()
      ),
      time = System.currentTimeMillis() / 1000
    )
  }
}
