package com.xk.bigdata.flink.datastream.datasource.custom

import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}

/**
 * custom Flink ：RichParallelSourceFunction
 */
object MysqlRichParallelSourceFunctionApp {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(4)
    val dataStream = env.addSource(new MysqlRichParallelSourceFunction)
    println(dataStream.parallelism)
    val mapStream = dataStream.map(x => x)
    println(mapStream.parallelism)
    mapStream.print()
    env.execute(this.getClass.getSimpleName)
  }

}
