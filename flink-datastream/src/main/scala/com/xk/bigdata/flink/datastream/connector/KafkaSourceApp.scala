package com.xk.bigdata.flink.datastream.connector

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer

/**
 * 使用 Kafka 作为数据源
 */
object KafkaSourceApp {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val properties = new Properties()
    val bootStrap = "bigdatatest02:9092,bigdatatest03:9092,bigdatatest04:9092"
    val sourceTopic = "flink_kafka_source"
    properties.setProperty("bootstrap.servers", bootStrap)
    properties.setProperty("group.id", "demo")
    val kafkaSource = new FlinkKafkaConsumer[String](sourceTopic, new SimpleStringSchema(), properties)
    val stream = env
      .addSource(kafkaSource)
    stream.flatMap(_.split(","))
        .map((_,1))
        .keyBy(_._1)
        .sum(1)
        .print()
    env.execute(this.getClass.getSimpleName)
  }

}
