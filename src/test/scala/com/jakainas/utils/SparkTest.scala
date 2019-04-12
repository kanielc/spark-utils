package com.jakainas.utils

import org.apache.spark.sql.SparkSession
import org.scalatest.{FunSuite, Matchers}

trait SparkTest extends FunSuite with Matchers {
  val spark = SparkSession.builder().appName("test")
    .master("local[*]").enableHiveSupport().getOrCreate()

  spark.sparkContext.setLogLevel("WARN")
}

