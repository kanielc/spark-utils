package com.jakainas

import java.text.SimpleDateFormat
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit

import com.jakainas.table.Table
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.IOUtils
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{ArrayType, DataType, MapType, StructType}
import org.apache.spark.sql._

import scala.reflect.runtime.universe._

package object functions {
  /**
   * Returns a default value for a given column if it's null, otherwise return the column's value
   *
   * @param c       - Column to be checked
   * @param default - Value to return if `c` is null
   * @return c if not null and default otherwise
   */
  def nvl[C1](c: C1, default: Any)(implicit ev: C1 => Column): Column = when(c.isNotNull, c).otherwise(default)

  /**
   * Provides a literal of String with a value of null
   *
   * @return `null` as a column of type String
   */
  def null_string: Column = lit(null.asInstanceOf[String])

  /**
   * Returns the schema for a type (e.g. case class, string, etc)
   *
   * @tparam T - The type whose schema is to be returned
   * @return The schema for a Dataset built with that type
   */
  def schemaFor[T: TypeTag]: StructType = {
    val dataType = ScalaReflection.schemaFor[T].dataType
    dataType.asInstanceOf[StructType]
  }

  /**
   * Adds days to a given date
   *
   * @param date    - date to be added to in the format of '2019-01-20'
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
   *
   * @return Today's date in UTC(String)
   */
  def today: String = LocalDate.now().toString

  /**
   * Yesterday as a string in UTC
   *
   * @return Yesterday's date in UTC(String)
   */
  def yesterday: String = LocalDate.now().minusDays(1).toString

  /**
   * Generate a string Date column by combining year, month, and day columns.
   * If one or more column is null, the result Date will be null.
   *
   * @param year  - the year column
   * @param month - the month column
   * @param day   - the day column
   * @return a string column consist of the combination of year, month, and day into date
   */
  def to_date_str(year: Column, month: Column, day: Column): Column = {
    date_format(concat(year, lit("-"), month, lit("-"), day), "yyyy-MM-dd")
  }

  /**
   * Convert a dateRange into its corresponding SQL query
   * dateRangeToSql("2017-01-09", "2019-04-10") returns: "(year = 2017 and month > 1) or (year = 2017 and month = 1 and day >= 9) or year = 2018 or (year = 2019 and ((month < 4) or (month = 4 and day <= 10)))"
   *
   * @param date1 - some date ('yyyy-mm-dd')
   * @param date2 - some date ('yyyy-mm-dd')
   * @return SQL query in string form
   */
  def dateRangeToSql(date1: String, date2: String): String = {
    def expandDate(date: String) = {
      val d = LocalDate.parse(date, DateTimeFormatter.ISO_DATE)
      Seq(d.getYear, d.getMonthValue, d.getDayOfMonth)
    }

    def range(start: Int, end: Int) = end - start match {
      case 2 => Seq(start + 1)
      case x if x > 2 => Seq(start + 1, end - 1)
      case _ => Seq.empty
    }

    val dateFormat = new SimpleDateFormat("yyyy-MM-dd")
    val check = dateFormat.parse(date1).compareTo(dateFormat.parse(date2))

    check match {
      case 0 =>
        val start = expandDate(date2)
        s"year = ${start.head} and month = ${start(1)} and day = ${start(2)}"
      case x if x > 0 =>
        dateRangeToSql(date2, date1)
      case _ =>
        val start = expandDate(date1)
        val end = expandDate(date2)

        val fullYears = range(start.head, end.head)

        val startSpec = s"(year = ${start.head} and (month > ${start(1)} or (month = ${start(1)} and day >= ${start(2)})))"
        val endSpec = s"(year = ${end.head} and (month < ${end(1)} or (month = ${end(1)} and day <= ${end(2)})))"

        val midYears = fullYears.length match {
          case 0 => " or "
          case 1 => s" or (year = ${fullYears.head}) or "
          case _ => s" or (year between ${fullYears.head} and ${fullYears.last}) or "
        }

        if (fullYears.nonEmpty || start.head != end.head) { // crosses years
          s"$startSpec$midYears$endSpec"
        } else { // just same year
          val monthDay = if (start(1) == end(1)) {
            s"(month = ${start(1)} and day between ${start(2)} and ${end(2)})"
          } else {
            val midMonthText = {
              val fullMonths = range(start(1), end(1))
              if (fullMonths.nonEmpty) s" or (month between ${fullMonths.head} and ${fullMonths.last})" else ""
            }

            s"((month = ${start(1)} and day >= ${start(2)})$midMonthText or (month = ${end(1)} and day <= ${end(2)}))"
          }

          s"(year = ${start.head} and $monthDay)"
        }
    }
  }

  /**
   * Converts a timestamp into bigint (long) in milliseconds (default Spark returns only seconds)
   *
   * @param timestamp - Timestamp to extract milliseconds from
   * @return Milliseconds since epoch for given timestamp
   */
  def millis(timestamp: Column): Column = (timestamp.cast("double") * 1000).cast("bigint")

  /**
   * countDistinct equivalent for window operations.
   * Distinct window operations are not currently supported by Spark so we can't just use countDistinct.
   *
   * TODO: see if performance can be improved using a UDAF or some other approach.
   *
   * @param countCol : Column values to count.
   * @param partCols : One or more columns to partition the dataset by.
   * @return : Count of unique values in countCol per partitionCols.
   */
  def countUnique(countCol: Column, partCols: Column*): Column = {
    size(collect_set(countCol).over(Window.partitionBy(partCols: _*)))
  }

  implicit class DatasetFunctions[T](private val ds: Dataset[T]) extends AnyVal {

    import ds.sparkSession.implicits._

    /**
     * Remove duplicate rows using some column criteria for grouping and ordering
     *
     * @param partCols - How to group rows.  Only 1 row from each group will be in the result
     * @param order    - How to sort rows.  The row with the first order position will be kept
     * @param conv     - Optional: Encoder for T, allowing us to work with both Dataset and DataFrames
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
     * Compares two Spark DataTypes to determine if they are the same or not, ignoring nullability.
     */
    private def sameDataType(x: DataType, y: DataType): Boolean = {
      (x, y) match {
        case (a: ArrayType, b: ArrayType) => sameDataType(a.elementType, b.elementType)
        case (a: MapType, b: MapType) => sameDataType(a.keyType, b.keyType) & sameDataType(a.valueType, b.valueType)
        case (a: StructType, b: StructType) => a.fields.zip(b.fields).map { case (d, e) => sameDataType(d.dataType, e.dataType) }.reduce(_ & _)
        case (a: DataType, b: DataType) => a.typeName == b.typeName
      }
    }

    /**
     * Converts the dataframe into the schema matching U by selecting the fields and then calling `as`.
     * Throws an exception at run time if column types are incorrect, ignoring nullability.
     *
     * @tparam U - The type whose schema to align to.
     * @return a Dataset of type U.
     */
    def cast[U: Encoder: TypeTag]: Dataset[U] = {
      val expSchema = schemaFor[U]

      val expFields = expSchema.fields.sortBy(_.name)
      val expFieldNamesSet = expSchema.fieldNames.toSet
      val curFields = ds.schema.fields.filter(x => expFieldNamesSet.contains(x.name)).distinct.sortBy(_.name)

      val wrongDataTypes = expFields.zip(curFields).collect {
        case (x, y) if !sameDataType(x.dataType, y.dataType) => s"DataType for '${x.name}' column doesn't match expected DataType in '${typeOf[U].toString}' schema.\nExpected: ${x.dataType}\nReceived: ${y.dataType}"
      }.mkString("\n")

      if (!wrongDataTypes.isEmpty) {
        throw new IllegalArgumentException(wrongDataTypes)
      }

      val expFieldNames = expSchema.fieldNames
      ds.select(expFieldNames.head, expFieldNames.tail: _*).as[U]
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

    def oldsaveCsv(path: String, includeHeader: Boolean = true): Unit = {
      val tempPath = path + "-tmp"
      val outFile = new Path(path)
      val ls = System.lineSeparator()

      val fs = FileSystem.get(outFile.toUri, ds.sparkSession.sparkContext.hadoopConfiguration)

      try {
        ds.write.option("header", "false").mode("overwrite").csv(tempPath)

        val header = if (includeHeader) ds.columns.map(c => s""""$c"""").mkString(",") + ls else ""
        val text = header + ds.sparkSession.read.text(tempPath).as[String].collect().mkString(ls)

        println(outFile.toUri)
        println(ds.sparkSession.sparkContext.hadoopConfiguration.get("fs.defaultFS"))

        val fullPath = ds.sparkSession.sparkContext.hadoopConfiguration.get("fs.defaultFS") + outFile.toString
        fs.create(new Path(fullPath)).writeBytes(text)
      } catch {
        case e: Throwable =>
          fs.delete(outFile, true)
          println("failed")
          throw e
      } finally {
        fs.delete(new Path(tempPath), true)
        fs.close()
      }
    }

    /**
     * Export a Dataset to a single CSV file (using Hadoop to copy the contents)
     *
     * @param path - location to write the single CSV file to, overwriting anything already there.
     */
    def saveCsv(path: String): Unit = {
      val tempPath = new Path(path + "-temp")
      val outFile = new Path(path)

      ds.write.option("header", "false").mode("overwrite").csv(tempPath.toString)

      val conf = ds.sparkSession.sparkContext.hadoopConfiguration
      val fs = FileSystem.get(outFile.toUri, conf)
      fs.deleteOnExit(tempPath)

      val output = fs.create(outFile, true)

      // write header
      output.write(s"${ds.columns.mkString(",")}\n".getBytes("UTF-8"))

      // copy other files
      fs.listStatus(tempPath).foreach {
        case file if !file.isDirectory =>
          IOUtils.copyBytes(fs.open(file.getPath), output, conf, false)
        case _ => Unit
      }

      output.close()
      fs.close()
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
     *
     * @param startDate - What specific date partition to start loading from (will only use year, month, day as available in schema) - Optional
     * @param endDate   - End of date range to load.  Will default to startDate if not provided.  If no startDate, this will be ignored.
     * @tparam T - The type to load, must have a `Table` definition available
     * @return Dataset of T representing data from the given path
     */
    def load[T: Encoder: Table](startDate: String = null, endDate: String = null): Dataset[T] = {
      val table = implicitly[Table[T]]

      val data = spark.read.option("basePath", table.basePath).parquet(table.fullPath)

      val res = Option(startDate).map { date =>
        val filter = dateRangeToSql(date, Option(endDate).getOrElse(startDate))
        println(filter)
        data.where(filter)
      }.getOrElse(data).as[T]

      res
    }

    /**
     * Loads CSV file(s) from a given location, including header and inferring schema
     *
     * @param paths - single or multiple file locations to load CSV data from
     * @return Dataset[Row] containing the data in the CSVs
     */
    def readCsv(paths: String*): Dataset[Row] = spark.read.options(Map("header" -> "true", "inferSchema" -> "true")).csv(paths: _*)
  }

}
