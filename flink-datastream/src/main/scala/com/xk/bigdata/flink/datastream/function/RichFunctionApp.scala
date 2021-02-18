package com.xk.bigdata.flink.datastream.function

import com.xk.bigdata.flink.datastream.datasource.domain.Domain.test
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._

/**
 * RichFunction
 */
object RichFunctionApp {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.readTextFile("data/test.txt")
      .map(new MyMapFunction)
      .print()
    env.execute(this.getClass.getSimpleName)
  }

  class MyMapFunction extends RichMapFunction[String, test] {
    override def map(value: String): test = {
      val splits = value.split(",")
      test(splits(0).toInt, splits(1))
    }
  }

}
