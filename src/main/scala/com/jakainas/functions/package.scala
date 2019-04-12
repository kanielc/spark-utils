package com.jakainas

import java.time.LocalDate
import java.time.format.DateTimeFormatter

import com.jakainas.table.{Table, TableConfig}
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Column, Dataset, Encoder, SparkSession}
import org.apache.spark.sql._

import scala.reflect.runtime.universe._

package object functions {
  /**
    * Returns a default value for a given column if it's null, otherwise return the column's value
    * @param c - Column to be checked
    * @param default - Value to return if `c` is null
    * @return c if not null and default otherwise
    */
  def nvl[C1](c: C1, default: Any)(implicit ev: C1 => Column): Column = when(c.isNotNull, c).otherwise(default)

  /**
    * Returns the schema for a type (e.g. case class, string, etc)
    * @tparam T - The type whose schema is to be returned
    * @return The schema for a Dataset built with that type
    */
  def schemaFor[T : TypeTag]: StructType = {
    val dataType = ScalaReflection.schemaFor[T].dataType
    dataType.asInstanceOf[StructType]
  }

  /**
    * Adds days to a given date
    * @param date - date to be added to in the format of '2019-01-20'
    * @param numDays - number of days to add, can be negative
    * @return numDays after (or before if negative) `date`
    */
  def plusDays(date: String, numDays: Int): String = {
    LocalDate.parse(date, DateTimeFormatter.ISO_DATE).minusDays(-numDays).toString
  }

    /**
    * Generate a string Date column by combining year, month, and day columns.
    * If one or more column is null, the result Date will be null.
    * @param year - the year column
    * @param month - the month column
    * @param day - the day column
    * @return a string column consist of the combination of year, month, and day into date
    */
  def to_date_str(year: Column, month: Column, day: Column): Column = {
    date_format(concat(year, lit("-"), month, lit("-"), day), "yyyy-MM-dd")
  }
  
  implicit class DatasetFunctions[T](val ds: Dataset[T]) extends AnyVal {
    /**
      * Remove duplicate rows using some column criteria for grouping and ordering
      * @param partCols - How to group rows.  Only 1 row from each group will be in the result
      * @param order - How to sort rows.  The row with the first order position will be kept
      * @param conv - Optional: Encoder for T, allowing us to work with both Dataset and DataFrames
      * @return The data with only 1 row per group based on given sort order
      */
    def distinctRows(partCols: Seq[Column], order: Seq[Column])(implicit conv: Encoder[T] = null): Dataset[T] = {
      var i = 0
      var colName = s"rn$i"

      while (ds.columns.contains(colName)) {
        i += 1
        colName = s"rn$i"
      }

      val deduped = ds.withColumn(s"rn$i", row_number over Window.partitionBy(partCols:_*)
        .orderBy(order:_*)).where(col(colName) === 1).drop(colName)

      if (conv == null)
        deduped.asInstanceOf[Dataset[T]]
      else deduped.as[T](conv)
    }

    /**
      * Add multiple new columns to the current DataFrame
      * (i.e., `withColumn` for a sequence of (String, Column) Tuples).
      *
      * @param newColTuples - a list of name-value Tuples2 (colName: String, colVal: Column).
      * @return - The DataFrame with new columns added.
      */
    def addColumns(newColTuples: (String, Column)*): DataFrame = newColTuples.foldLeft(ds.toDF()) {
      // From left to right, for each new (colName, colVal) Tuple add it to the current DataFrame
      case (newDF, (colName, colVal)) => newDF.withColumn(colName, colVal)
    }

    /**
      * Rename multiple new columns of the current DataFrame
      * (i.e., `withColumnRenamed` for a sequence of (String, String) Tuples).
      *
      * @param renameColTuples - a list of current, new column name Tuples2 (currColName: String, newColName: String).
      * @return - The DataFrame with mulitple renamed columns.
      */
    def renameColumns(renameColTuples: (String, String)*): DataFrame = renameColTuples.foldLeft(ds.toDF()) {
      // From left to right, for each new (currColName, newColName) Tuple apply withColumnRenamed
      case (newDF, (currColName, newColName)) => newDF.withColumnRenamed(currColName, newColName)
    }

    def save()(implicit table: Table[T]): Unit = {
      ds.write.partitionBy(table.partitioning:_*).parquet(table.fullPath)
    }
  }

  implicit class SparkFunctions(val spark: SparkSession) extends AnyVal {
    def load[T : Encoder : Table]: Dataset[T] = {
      val table = implicitly[Table[T]]

      spark.read.option("basePath", table.basePath).parquet(table.fullPath).as[T]
    }
  }

}
