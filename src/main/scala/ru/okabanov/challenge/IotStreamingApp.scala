package ru.okabanov.challenge

import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Duration, Seconds, StreamingContext}
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods._
import ru.okabanov.challenge.dao.{IotDeviceDao, IotDeviceDaoImpl}
import ru.okabanov.challenge.model.InputLog

/**
  * @author okabanov
  */
object IotStreamingApp {
  private implicit val formats = DefaultFormats
  private lazy val sparkConf = new SparkConf()

  case class AppOptions(batchDuration: Duration, kafkaInputTopic: String, kafkaBrokers: String, kafkaGroupId: String)

  def main(args: Array[String]): Unit = {
    val options = buildAppOptions
    val ssc = new StreamingContext(sparkConf, options.batchDuration)

    val inputStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc,
      createKafkaParams(options),
      Set(options.kafkaInputTopic)
    )
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
          getIotDeviceDao.saveBatch(partition.toSeq)
        }
      }
  }

  private def getIotDeviceDao: IotDeviceDao = {
    new IotDeviceDaoImpl()
  }

  private def createKafkaParams(appOptions: AppOptions): Map[String, String] = {
    Map[String, String](
      "bootstrap.servers" -> appOptions.kafkaBrokers,
      "key.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
      "value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
      "group.id" -> appOptions.kafkaGroupId,
      "auto.offset.reset" -> "largest",
      "enable.auto.commit" -> "true"
    )
  }

  private def buildAppOptions: AppOptions = {
    AppOptions(
      batchDuration = Seconds(sparkConf.get("spark.iot-log-parser.batch-duration", "5").toInt),
      kafkaInputTopic = sparkConf.get("spark.iot-log-parser.kafka.input-topic", "iot-device-log"),
      kafkaBrokers = sparkConf.get("spark.iot-log-parser.kafka.brokers"),
      kafkaGroupId = sparkConf.get("spark.iot-log-parser.kafka.group")
    )
  }
}
