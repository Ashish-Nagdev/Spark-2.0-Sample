package com.sherpa.examples.sparkdemo

import org.apache.spark.sql.SparkSession

/**
  * Created by Ashish Nagdev
  */
object DataSetWordCount {

  def main(args: Array[String]) {

    val sparkSession = SparkSession.builder
      .master("local")
      .appName("Word-Count-Example")
      .getOrCreate()

    import sparkSession.implicits._
    val data = sparkSession.read.text("src/main/resources/data.txt").as[String]

    val words = data.flatMap(value => value.split("\\s+"))

    val groupedWords = words.groupByKey(_.toLowerCase)

    val counts = groupedWords.count()

    counts.show()

  }

}
