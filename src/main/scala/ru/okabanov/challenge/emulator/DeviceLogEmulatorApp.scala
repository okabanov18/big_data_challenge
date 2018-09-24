package ru.okabanov.challenge.emulator

import java.io.FileInputStream
import java.util.Properties
import ru.okabanov.challenge.LogKafkaProducer

/**
  * @author okabanov
  */
object DeviceLogEmulator {

  def main(args: Array[String]): Unit = {
    val (brokers, topic) = readKafkaProps()
    val producer = new LogKafkaProducer(brokers)
    (1 to 3).foreach { i =>
      new DeviceEmulator(s"device-$i", producer, topic).start()
    }
  }

  private def readKafkaProps() = {
    val prop = new Properties()
    prop.load(new FileInputStream("spark_application.conf"))

    (
      prop.getProperty("spark.iot-log-parser.kafka.brokers"),
      prop.getProperty("spark.iot-log-parser.kafka.input-topic")
    )
  }
}
