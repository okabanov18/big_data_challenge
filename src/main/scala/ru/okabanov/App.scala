package ru.okabanov

import kafka.serializer.{DefaultDecoder, StringDecoder}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.json4s.DefaultFormats
import org.json4s.jackson.{JsonMethods, Serialization}

import scala.util.{Failure, Success, Try}

import org.json4s.jackson.JsonMethods._

/**
 * @author ${user.name}
 */
object App {

  implicit val formats = DefaultFormats
  
  def main(args : Array[String]) {
    val sparkConf = new SparkConf()
    val batchDuration = Seconds(sparkConf.get("spark.log-parser.batch-duration", "5").toInt)
    val kafkaInputTopic = sparkConf.get("spark.log-parser.kafka.input-topic", "iot-device-log")
    val kafkaBrokers = sparkConf.get("spark.log-parser.kafka.brokers", "quickstart.cloudera:9092")
    val kafkaGroupId = sparkConf.get("spark.log-parser.kafka.group", "iot-device-group")
    val windowDuration = sparkConf.getInt("spark.log-parser.window.duration.seconds", 60)

    lazy val logKafkaProducer = new LogKafkaProducer(kafkaBrokers)

    val ssc = new StreamingContext(sparkConf, batchDuration)
    val kafkaParams = createKafkaParams(kafkaBrokers, kafkaGroupId)

    val inputStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, Set(kafkaInputTopic))

    inputStream
      .flatMap { case(id, value) =>
        implicit val formats = DefaultFormats
        parse(value).extract[Array[InputLog]].map(_.data)
      }.foreachRDD { rdd =>
        rdd.foreachPartition { partition =>
          val iotDeviceDao = new IotDeviceDao()
          iotDeviceDao.init()
          partition.foreach { iotDeviceDao.save }
          iotDeviceDao.close()
        }
    }

    ssc.start()
    ssc.awaitTermination()
  }

  def createKafkaParams(kafkaBrokers: String, groupId: String): Map[String, String] = {
    Map[String, String](
      "bootstrap.servers" -> kafkaBrokers,
      "key.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
      "value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
      "group.id" -> groupId,
      "auto.offset.reset" -> "largest",
      "enable.auto.commit" -> "true"
    )
  }

  def parseLogs(line: String): Seq[InputLog] = {
    Try(JsonMethods.parse(line).extract[Seq[InputLog]]) match {
      case Success(logs) => logs
      case Failure(e) => //logger.warn(s"error parsing message: $line, error: ${e.getLocalizedMessage}");
        Seq.empty
    }
  }
}
