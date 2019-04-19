package com.jakainas.utils

import java.util.TimeZone

import org.apache.spark.sql.SparkSession
import org.scalatest.{FunSuite, Matchers}

trait SparkTest extends FunSuite with Matchers {
  val spark = SparkSession.builder().appName("test")
    .master("local[*]").enableHiveSupport().getOrCreate()

  TimeZone.setDefault(TimeZone.getTimeZone("UTC"))
  spark.sparkContext.setLogLevel("WARN")
}

