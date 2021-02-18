package com.xk.bigdata.flink.datastream.datasource.buildin

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._

/**
 * Flink Build-in Read File DataSource
 */
object FileDataSourceApp {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    // 设置 env 的并行度
    env.setParallelism(4)
    val fileStream = env.readTextFile("data/wc.txt")
    // 得到 fileStream 的并行度
    println(fileStream.parallelism)
    val mapStream = fileStream.map(x => x)
    // 得到 mapStream 的并行度
    println(mapStream.parallelism)

    env.execute(this.getClass.getSimpleName)
  }

}
