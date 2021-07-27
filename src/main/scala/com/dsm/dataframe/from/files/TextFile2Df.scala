package com.dsm.dataframe.from.files

import com.dsm.utils.Constants
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.types._

object TextFile2Df {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .master("local[*]")
      .appName("Dataframe Example")
      .getOrCreate()
    spark.sparkContext.setLogLevel(Constants.ERROR)
    val rootConfig = ConfigFactory.load("application.conf").getConfig("conf")
    val s3Config = rootConfig.getConfig("s3_conf")
    spark.sparkContext.hadoopConfiguration.set("fs.s3n.awsAccessKeyId", s3Config.getString("access_key"))
    spark.sparkContext.hadoopConfiguration.set("fs.s3n.awsSecretAccessKey", s3Config.getString("secret_access_key"))

    println("\nCreating dataframe from CSV file using 'SparkSession.read.format()',")
    val finSchema = new StructType()
      .add("id", IntegerType,true)
      .add("has_debt", BooleanType,true)
      .add("has_financial_dependents", BooleanType,true)
      .add("has_student_loans", BooleanType,true)
      .add("income", DoubleType,true)

    val finDf = spark.read
        .option("header", "false")
        .option("delimiter", ",")
        .format("csv")
        .schema(finSchema)
        .load("s3n://" + "sridattu-bigdata" + "/finances.csv")

    finDf.printSchema()
    finDf.show()

    println("Creating dataframe from CSV file using 'SparkSession.read.csv()',")
    val financeDf = spark.read
      .option("mode", "DROPMALFORMED")
      .option("header", "false")
      .option("delimiter", ",")
      .option("inferSchema", "true")
      .csv("s3n://" + "sridattu-bigdata" + "/finances.csv")
      .toDF("id", "has_debt", "has_financial_dependents", "has_student_loans", "income")

    println("# of partitions = " + finDf.rdd.getNumPartitions)
    financeDf.printSchema()
    financeDf.show()

    financeDf
      .repartition(2)
      .write
      .mode(SaveMode.Overwrite)
      .option("header", "true")
      .option("delimiter", "~")
      .csv("s3n://" + "sridattu-bigdata" + "/fin")

    spark.close()
  }
}
