package com.jakainas

import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit

import com.jakainas.table.Table
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Column, Dataset, Encoder, SparkSession, _}

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
   * Provides a literal of String with a value of null
   * @return `null` as a column of type String
   */
  def null_string: Column = lit(null.asInstanceOf[String])

  /**
   * Returns the schema for a type (e.g. case class, string, etc)
   * @tparam T - The type whose schema is to be returned
   * @return The schema for a Dataset built with that type
   */
  def schemaFor[T: TypeTag]: StructType = {
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
    parseDate(date).minusDays(-numDays).toString
  }

  /**
   * Converts a date string to a LocalDate
   *
   * @param date - date string to convert in the format of '2019-01-20'
   * @return LocalDate representation of the given date
   */
  def parseDate(date: String): LocalDate = {
    LocalDate.parse(date, DateTimeFormatter.ISO_DATE)
  }

  /**
   * Returns a list of dates that lie between two given dates
   *
   * @param start - start date (yyyy-mm-dd)
   * @param end   - end date (yyyy-mm-dd)
   * @return The dates between start and end in the form of a sequence of strings
   */
  def dateRange(start: String, end: String): IndexedSeq[String] = {
    val days = ChronoUnit.DAYS.between(parseDate(start), parseDate(end)).toInt
    require(days >= 0, s"Start date ($start) must be before end date ($end)!")
    (0 to days).map(d => plusDays(start, d))
  }

  /**
   * Today as a string in UTC
   * @return Today's date in UTC(String)
   */
  def today: String = LocalDate.now().toString

  /**
   * Yesterday as a string in UTC
   * @return Yesterday's date in UTC(String)
   */
  def yesterday: String = LocalDate.now().minusDays(1).toString

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

  /**
    * Converts a timestamp into bigint (long) in milliseconds (default Spark returns only seconds)
    * @param timestamp - Timestamp to extract milliseconds from
    * @return Milliseconds since epoch for given timestamp
    */
  def millis(timestamp: Column): Column = (timestamp.cast("double") * 1000).cast("bigint")

  implicit class DatasetFunctions[T](private val ds: Dataset[T]) extends AnyVal {
    import ds.sparkSession.implicits._

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

      val deduped = ds.withColumn(s"rn$i", row_number over Window.partitionBy(partCols: _*)
        .orderBy(order: _*)).where(col(colName) === 1).drop(colName)

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
      ds.write.partitionBy(table.partitioning: _*).parquet(table.fullPath)
    }

    /**
     * Calculate the frequency of given columns and sort them in descending order
     *
     * @param  columns - column(s) to count by
     * @return - Dataframe with given columns soft by descending order of count
     */
    def frequency(columns: Column*): DataFrame = {
      ds.groupBy(columns: _*).agg(count("*") as "count").sort('count.desc)
    }

    /**
     * Calculate the frequency of given columns and sort them in descending order
     *
     * @param col1 - First column to group by
     * @param cols - remaining columns to count by (Optional)
     * @return - The DataFrame with columns and the respective frequency of the columns
     */
    def frequency(col1: String, cols: String*): DataFrame = {
      ds.groupBy(col1, cols: _*).agg(count("*") as "count").sort('count.desc)
    }
  }

  implicit class SparkFunctions(val spark: SparkSession) extends AnyVal {
    /**
     * Dynamically loads a case class representing a table from its provided location
     * @param date - What specific date partition to load from (will only use year, month, day as available in schema) - Optional
     * @tparam T - The type to load, must have a `Table` definition available
     * @return Dataset of T representing data from the given path
     */
    def load[T: Encoder: Table](date: String = null): Dataset[T] = {
      val table = implicitly[Table[T]]

      val data = spark.read.option("basePath", table.basePath).parquet(table.fullPath)

      Option(date).map { date =>
        val dateVal = parseDate(date)
        val schema = table.schema
        val filters = Seq("year", "month", "day")

        schema.collect {
          case f if filters.contains(f.name) =>
            f.name match {
              case "year" => s"year = ${dateVal.getYear}"
              case "month" => s"month = ${dateVal.getMonthValue}"
              case "day" => s"day = ${dateVal.getDayOfMonth}"
            }
        }.mkString(" and ")
      }.fold(data) { filt => data.where(filt) }.as[T]
    }

    /**
     * Loads CSV file(s) from a given location, including header and inferring schema
     * @param paths - single or multiple file locations to load CSV data from
     * @return Dataset[Row] containing the data in the CSVs
     */
    def readCsv(paths: String*): Dataset[Row] = spark.read.options(Map("header" -> "true", "inferSchema" -> "true")).csv(paths: _*)
  }
}
