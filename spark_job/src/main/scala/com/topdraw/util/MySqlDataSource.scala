package com.topdraw.util

import com.alibaba.druid.pool.DruidDataSource


object MySqlDataSource {

  val driver = "com.mysql.jdbc.Driver"
  val url = "jdbc:mysql://139.196.37.202:3306/cy_test"
  val username = "druid_read"
  val password = "Topdraw1qaz"
  val connectionPool = new DruidDataSource()
  connectionPool.setUsername(username)
  connectionPool.setPassword(password)
  connectionPool.setDriverClassName(driver)
  connectionPool.setUrl(url)
  connectionPool.setValidationQuery("select 1")
  connectionPool.setInitialSize(15)
  connectionPool.setMinIdle(10)
  connectionPool.setMaxActive(100)
  connectionPool.setRemoveAbandoned(true)
  connectionPool.setRemoveAbandonedTimeoutMillis(180000)
  connectionPool.setMaxWait(5000)
  connectionPool.setTestOnBorrow(false)
  connectionPool.setTestOnReturn(false)

}
