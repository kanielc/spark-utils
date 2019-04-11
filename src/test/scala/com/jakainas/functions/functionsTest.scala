package com.jakainas.functions

import com.jakainas.functions.functionsTest.TestData

class functionsTest extends SparkTest {
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

  test("addColumns to a DataFrame") {
    val inputDF = Seq(("a", "b", "c"), ("d", "e", "f")).toDF("dfCol1", "dfCol2", "dfCol3")
    val resultDF = inputDF.addColumns(("dfCol1plus1", ('dfCol1 + 1)), ("dfCol2x2", ($"dfCol2" * 2)))

    // test column names and values are as expected
    resultDF.columns should contain theSameElementsAs Array("dfCol1", "dfCol2", "dfCol3", "dfCol1plus1", "dfCol2x2")
    resultDF.where('dfCol1plus1 =!= ('dfCol1 + 1) or 'dfCol2x2 =!= ('dfCol2 * 2)).count shouldEqual 0

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

}

object functionsTest {
  case class TestData(x: String, y: Int)
}
