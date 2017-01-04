package com.sherpa.examples.sparkdemo

import org.apache.spark.sql.SparkSession


/**
  * Created by Ashish.Nagdev
  * Catalog Example
  */
object CatalogExample {

  def main(args: Array[String]) {

    val sparkSession = SparkSession.builder.
           master("local")
           .appName("Catalog_Example")
           .getOrCreate()


    sparkSession.sparkContext.setLogLevel("ERROR")
    val df = sparkSession.read.csv("src/main/resources/sales.csv")
    df.createTempView("sales")

    //interacting with catalogue

    val catalog = sparkSession.catalog

    //print the databases
    println("Databases:")
    catalog.listDatabases().select("name").show()

    // print all the tables
    println("Tables:")
    catalog.listTables().select("name").show()

    // is cached
    println("Is Catalog Cached? - "+ catalog.isCached("sales"))
    df.cache()
    println("Is Catalog Cached? - "+ catalog.isCached("sales"))

    // drop the table
    catalog.dropTempView("sales")
    println("After dropping temp view, Tables: ")
    catalog.listTables().select("name").show()

    // list functions
    println("Functions:")
    catalog.listFunctions().select("name","description","className","isTemporary").show(100)
  }

}
