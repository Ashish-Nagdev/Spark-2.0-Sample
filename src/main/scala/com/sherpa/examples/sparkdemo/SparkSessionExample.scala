package com.sherpa.examples.sparkdemo

import org.apache.spark.sql.SparkSession

/**
  * Created by Ashish Nagdev
  * Spark Session example
  */
object SparkSessionSample {

  def main(args: Array[String]) {

    val sparkSession = SparkSession.builder
      .master("local")
      .appName("spark session example")
      .getOrCreate()

    val dataFrame = sparkSession.read.option("header", "true").csv("src/main/resources/sales.csv")

    dataFrame.show()

  }

}
