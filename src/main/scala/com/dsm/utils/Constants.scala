package com.dsm.utils

import com.typesafe.config.{Config, ConfigFactory}

object Constants {
  val rootConfig = ConfigFactory.load("application.conf").getConfig("conf")
  val s3Config = rootConfig.getConfig("s3_conf")
  
  val ACCESS_KEY = s3Config.getString("access_key")
  val SECRET_ACCESS_KEY = s3Config.getString("secret_access_key")
  val S3_BUCKET = s3Config.getString("s3_bucket")
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
