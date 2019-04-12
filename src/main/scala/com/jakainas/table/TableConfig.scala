package com.jakainas.table

trait TableConfig {
  type Table
  def partitioning: Seq[String]
  def basePath: String
  def tableName: String
  def saveMode: WriteMode = WriteMode.PART_OVERWRITE
}
