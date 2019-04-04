package com.sparkdboffload

import org.apache.spark.sql.SparkSession

object SparkDBOffloadApp {
  def main(args: Array[String]): Unit = {

    println("---------Start-------------")
    val spark = SparkSession
      .builder()
      .appName("My Sample Spark Project")
      .master("local[*]")
//    .master("yarn")
      .getOrCreate()

/*
    val mysqlDF = spark.read
      .format("jdbc")
      .option("url", "jdbc:mysql://10.20.30.98:3306/retail_db")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("dbtable", "customers")
      .option("user", "mapr")
      .option("password", "mapr")
      .load()

    mysqlDF.coalesce(1).write.mode("overwrite").parquet("/tmp/sparkdb.output")
*/

        //val sqlContext = new org.apache.spark.sql.SQLContext(spark.sparkContext)
	val jdbcDbTable = "kp_test"
 
	val jdbcSqlConnStr = "jdbc:jtds:sqlserver://sqltest_dev_r_2016:4000;integratedSecurity=true;authenticationScheme=JavaKerberos;databaseName=EDA;user=nmtest;password=nmtest"
 
	val jdbcDF = spark.read
			.format("jdbc")
      			.option("url", jdbcSqlConnStr)
      			.option("dbtable", jdbcDbTable)
			.load()

        jdbcDF.printSchema
        jdbcDF.show

  }
}
