package com.xk.bigdata.flink.datastream.datasource.buildin

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.LongValueSequenceIterator

/**
 * Flink Build-in Read Collection DataSource
 */
object CollectionDataSource {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    // 设置 env 的并行度
    env.setParallelism(4)

    // fromElements
    val dataStream1 = env.fromElements("spark", 1L, 2D, '1')
    println(dataStream1.parallelism)
    val mapStream1 = dataStream1.map(x => x)
    println(mapStream1.parallelism)

    // fromCollection
    val dataStream2 = env.fromCollection(List("spark,hadoop", 1L))
    println(dataStream2.parallelism)
    val mapStream2 = dataStream2.map(x => x)
    println(mapStream2.parallelism)

    // fromParallelCollection
    val dataStream3 = env.fromParallelCollection(new LongValueSequenceIterator(1L, 10L))
    println(dataStream3.parallelism)
    val mapStream3 = dataStream3.map(x => x)
    println(mapStream3.parallelism)

    env.execute(this.getClass.getSimpleName)
  }

}
