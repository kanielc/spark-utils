package com.jakainas.functions

import com.jakainas.table.{DailyPartitioning, Table, TableConfig}
import com.jakainas.utils.SparkTest
import org.apache.commons.io.FileUtils

class functionsTest extends SparkTest {
  import functionsTest._
  import org.apache.spark.sql.functions._
  import spark.implicits._

  test("distinct by removes duplicates") {
    // with a dataset
    Seq("a", "b", "a").toDS().distinctRows(Seq('value), Seq(lit(true)))
      .as[String].collect should contain theSameElementsAs Array("a", "b")

    // with a dataframe
    Seq("a", "b", "a").toDF("value").distinctRows(Seq('value), Seq(lit(true)))
      .collect().map(_.getString(0)) should contain theSameElementsAs Array("a", "b")

    // multi-column case class
    Seq(TestData("a", 7), TestData("b", 3), TestData("a", 2)).toDS
      .distinctRows(Seq('x), Seq('y.desc))
      .collect() should contain theSameElementsAs Array(TestData("a", 7), TestData("b", 3))
  }

  test("nvl replaces null") {
    Seq("a", "b", null).toDS.select(nvl('value, "c"))
      .as[String].collect should contain theSameElementsAs Array("a", "b", "c")

    Seq(("a", "a1"), (null, "b1")).toDF("a", "b").withColumn("a", nvl('a, 'b))
      .as[(String, String)].collect should contain theSameElementsAs Array(("a", "a1"), ("b1", "b1"))
  }

  test("can generate the schema of a case class") {
    val schema = Seq(TestData("a", 7), TestData("b", 3)).toDS.schema

    schemaFor[TestData] shouldEqual schema
  }

  test("add days to a given date") {
    plusDays("2018-01-10", 5) shouldEqual "2018-01-15"
    plusDays("2018-01-10", -5) shouldEqual "2018-01-05"
    plusDays("2018-01-10", 0) shouldEqual "2018-01-10"
    an [NullPointerException] should be thrownBy plusDays(null, 5)
  }

  test("to_date_str: numeric input, should give normal output"){
    Seq(("2019", "04", "10")).toDF("year", "month", "day").select(to_date_str('year, 'month, 'day))
      .as[String].collect  should contain theSameElementsAs Array("2019-04-10")

    Seq((2019, 4, 10)).toDF("year", "month", "day").select(to_date_str('year, 'month, 'day))
      .as[String].collect  should contain theSameElementsAs Array("2019-04-10")
  }

  test("to_date_str: text input, should return null"){
    Seq(("2019", "Four", "Ten")).toDF("year", "month", "day").select(to_date_str('year, 'month, 'day))
      .as[String].collect  should contain theSameElementsAs Array(null)
  }

  test("to_date_str: null input, should return null"){
    Seq(("2019", "04", null)).toDF("year", "month", "day").select(to_date_str('year, 'month, 'day))
      .as[String].collect  should contain theSameElementsAs Array(null)
  }

  test("to_date_str: out of range input, should return null"){
    Seq(("2019", "13", "32")).toDF("year", "month", "day").select(to_date_str('year, 'month, 'day))
      .as[String].collect  should contain theSameElementsAs Array(null)
  }
    
  test("addColumns to a DataFrame") {
    val inputDF = Seq((1, 2, 3), (2, 4, 8)).toDF("dfCol1", "dfCol2", "dfCol3")
    val resultDF = inputDF.addColumns(("dfCol1plus1", ('dfCol1 + 1)), ("dfCol2x2", ($"dfCol2" * 2)))
    val expectedDF = Seq((1, 2, 3, 2, 4), (2, 4, 8, 3, 8)).toDF("dfCol1", "dfCol2", "dfCol3", "dfCol1plus1", "dfCol2x2")

    // test column names and values are as expected
    resultDF.columns should contain theSameElementsAs Array( "dfCol1", "dfCol2", "dfCol3", "dfCol1plus1", "dfCol2x2")
    resultDF.collect should contain theSameElementsAs expectedDF.collect

    // with dataset as well
    Seq(TestData("a", 7), TestData("b", 3)).toDS()
      .addColumns("a" -> 'y * 10, "b" -> ('y + 100))
      .select('a, 'b).as[(Int, Int)].collect should contain theSameElementsAs Array((70, 107), (30, 103))
  }

  test("renameColumns within a DataFrame" ) {
    val inputDF = Seq((1, 2, 3), (2, 4, 8)).toDF("dfCol1", "dfCol2", "dfCol3")
    val resultDF = inputDF.renameColumns(Map("dfCol1" -> "col1", "dfCol2" -> "col2").toSeq: _*)
    val expectedDF = Seq((1, 2, 3), (2, 4, 8)).toDF("col11", "col2", "dfCol3")

    // test column names and values are as expected
    resultDF.columns should contain theSameElementsAs Array("col1", "col2", "dfCol3")
    resultDF.collect should contain theSameElementsAs expectedDF.collect

    // with dataset as well
    Seq(TestData("a", 7), TestData("b", 3)).toDS()
      .renameColumns("x" -> "a", "y" -> "b")
      .select('a, 'b).as[(String, Int)].collect should contain theSameElementsAs Array(("a", 7), ("b", 3))
  }

  test("save and load works for tables") {
    val raw = Seq(PartData("a", 7, 2019, 1, 10), PartData("b", 3, 2018, 2, 5))

    FileUtils.deleteDirectory(new java.io.File("/tmp/footables/"))
    raw.toDS.save()
    spark.read.parquet("/tmp/footables/part-data").as[PartData].collect() should contain theSameElementsAs raw
    spark.load[PartData].collect() should contain theSameElementsAs raw
    FileUtils.deleteDirectory(new java.io.File("/tmp/footables"))
  }
}

object functionsTest {
  case class TestData(x: String, y: Int)
  object TestData {
    implicit val tableConf = new Table[TestData] with DailyPartitioning with LogTables
  }

  case class PartData(x: String, y: Int, year: Int, month: Int, day: Int)
  object PartData {
    implicit val tableConf: Table[PartData] = new Table[PartData] with DailyPartitioning with LogTables
  }

  trait LogTables extends TableConfig {
    override def basePath: String = "/tmp/footables"
  }
}
