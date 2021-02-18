package com.xk.bigdata.flink.datastream.datasource.custom

import java.sql.Statement

import com.alibaba.druid.pool.DruidPooledConnection
import com.xk.bigdata.flink.datastream.datasource.domain.Domain.test
import com.xk.bigdata.flink.datastream.utils.MysqlConnectPool
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, SourceFunction}

class MysqlRichParallelSourceFunction extends RichParallelSourceFunction[test] {

  var isRunning = true
  var connection: DruidPooledConnection = _
  var statement: Statement = _

  override def open(parameters: Configuration): Unit = {
    connection = MysqlConnectPool.getConnect
    statement = connection.createStatement()
  }

  override def close(): Unit = {
    statement.close()
    connection.close()
  }

  override def run(ctx: SourceFunction.SourceContext[test]): Unit = {
    if (isRunning) {
      val resultSet = statement.executeQuery("select * from test")
      while (resultSet.next()) {
        val id = resultSet.getInt("id")
        val name = resultSet.getString("name")
        ctx.collect(test(id, name))
      }
    }
  }

  override def cancel(): Unit = isRunning = false
}
