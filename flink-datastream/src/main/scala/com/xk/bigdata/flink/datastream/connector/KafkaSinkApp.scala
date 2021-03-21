package com.xk.bigdata.flink.datastream.connector

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer

/**
 * 数据输出到 Kafka
 */
object KafkaSinkApp {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val stream = env.readTextFile("data/wc.txt")
    val bootStrap = "bigdatatest02:9092,bigdatatest03:9092,bigdatatest04:9092"
    val topic = "flink_kafka_sink"

    val myProducer = new FlinkKafkaProducer[String](
      bootStrap, // target topic
      topic,
      new SimpleStringSchema()) // serialization schema

    stream.addSink(myProducer)
    env.execute(this.getClass.getSimpleName)
  }

}
