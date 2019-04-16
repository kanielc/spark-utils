package com.jakainas.table

import org.apache.spark.sql.types.StructType

trait TableConfig {
  type Table
  def partitioning: Seq[String]
  def basePath: String
  def tableName: String
  def saveMode: WriteMode = WriteMode.PART_OVERWRITE
  def schema: StructType
}
