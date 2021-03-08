package com.xk.bigdata.flink.datastream.partition

import com.xk.bigdata.flink.datastream.domain.City
import org.apache.flink.api.common.functions.Partitioner
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._

object MyPartitionApp {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(3)
    val stream = env.readTextFile("data/city.txt")
    val cityStream = stream.map(x => {
      val splits = x.split(",")
      City(splits(0), splits(1), splits(2))
    })
    cityStream.partitionCustom[String](new MyPartition, 0)
      .map(x => {
        println("thread id is " + Thread.currentThread().getId + "values:" + x)
        x
      })
      .print()
    env.execute(this.getClass.getSimpleName)
  }

}

class MyPartition extends Partitioner[String] {
  override def partition(key: String, numPartitions: Int): Int = {
    println("==========numPartitions:" + numPartitions)
    if (key == "浙江省") {
      0
    } else if (key == "安徽省") {
      1
    } else {
      2
    }
  }
}