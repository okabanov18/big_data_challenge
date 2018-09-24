package ru.okabanov.challenge

import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods
import org.json4s.jackson.JsonMethods._
import ru.okabanov.challenge.dao.IotDeviceDaoImpl

import scala.util.{Failure, Success, Try}

/**
  * @author okabanov
  */
object IotLogsStreamingProcessing {
  implicit val formats = DefaultFormats

  def run() {
    val sparkConf = new SparkConf()
    val batchDuration = Seconds(sparkConf.get("spark.iot-log-parser.batch-duration", "5").toInt)
    val kafkaInputTopic = sparkConf.get("spark.iot-log-parser.kafka.input-topic", "iot-device-log")
    val kafkaBrokers = sparkConf.get("spark.iot-log-parser.kafka.brokers")
    val kafkaGroupId = sparkConf.get("spark.iot-log-parser.kafka.group")

    val ssc = new StreamingContext(sparkConf, batchDuration)
    val kafkaParams = createKafkaParams(kafkaBrokers, kafkaGroupId)

    val inputStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, Set(kafkaInputTopic))

    processStream(inputStream)

    ssc.start()
    ssc.awaitTermination()
  }

  private[challenge] def processStream(inputStream: InputDStream[(String, String)]) = {
    inputStream
      .flatMap { case (id, value) =>
        implicit val formats = DefaultFormats
        parse(value).extract[Array[InputLog]].map(_.data)
      }
      .foreachRDD { rdd =>
        rdd.foreachPartition { partition =>
          val iotDeviceDao = getIotDeviceDao
          iotDeviceDao.init()
          partition.foreach{a => iotDeviceDao.save(a.copy(deviceId = "123123")) }
          iotDeviceDao.close()
        }
      }
  }

  private def getIotDeviceDao = {
    new IotDeviceDaoImpl()
  }

  private def createKafkaParams(kafkaBrokers: String, groupId: String): Map[String, String] = {
    Map[String, String](
      "bootstrap.servers" -> kafkaBrokers,
      "key.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
      "value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
      "group.id" -> groupId,
      "auto.offset.reset" -> "largest",
      "enable.auto.commit" -> "true"
    )
  }

  private def parseLogs(line: String): Seq[InputLog] = {
    Try(JsonMethods.parse(line).extract[Seq[InputLog]]) match {
      case Success(logs) => logs
      case Failure(e) => //logger.warn(s"error parsing message: $line, error: ${e.getLocalizedMessage}");
        Seq.empty
    }
  }
}
