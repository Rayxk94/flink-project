package com.xk.bigdata.flink.datastream.datasource.custom

import com.alibaba.druid.pool.DruidPooledConnection
import com.xk.bigdata.flink.datastream.datasource.domain.Domain.test
import com.xk.bigdata.flink.datastream.utils.MysqlConnectPool
import org.apache.flink.streaming.api.functions.source.SourceFunction

class MysqlSourceFunction extends SourceFunction[test] {

  var isRunning = true
  var connection: DruidPooledConnection = _

  override def run(ctx: SourceFunction.SourceContext[test]): Unit = {
    connection = MysqlConnectPool.getConnect
    val statement = connection.createStatement()
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
