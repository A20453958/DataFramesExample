package com.dsm.utils

import com.typesafe.config.Config

object Constants {
  val ACCESS_KEY = "AKIAYRTE73X6SDU75DYH"
  val SECRET_ACCESS_KEY = "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"
  val S3_BUCKET = "sridattu-bigdata"
  val ERROR = "ERROR"

  def getRedshiftJdbcUrl(redshiftConfig: Config): String = {
    val host = redshiftConfig.getString("host")
    val port = redshiftConfig.getString("port")
    val database = redshiftConfig.getString("database")
    val username = redshiftConfig.getString("username")
    val password = redshiftConfig.getString("password")
    s"jdbc:redshift://${host}:${port}/${database}?user=${username}&password=${password}"
  }

  // Creating Redshift JDBC URL
  def getMysqlJdbcUrl(mysqlConfig: Config): String = {
    val host = mysqlConfig.getString("hostname")
    val port = mysqlConfig.getString("port")
    val database = mysqlConfig.getString("database")
    s"jdbc:mysql://$host:$port/$database?autoReconnect=true&useSSL=false"
  }

}
