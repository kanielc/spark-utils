package com.jakainas

import java.time.LocalDate
import java.time.format.DateTimeFormatter

import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Column, Dataset, Encoder}

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

  implicit class DatasetFunctions[T](val ds: Dataset[T]) {
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
  }
}
