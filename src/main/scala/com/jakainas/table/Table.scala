package com.jakainas.table

import org.apache.spark.sql.types.StructType

import scala.reflect.runtime.universe

object Table {
  def defaultTableName[T: universe.TypeTag]: String = {
    universe.typeOf[T].toString
      .replace("$", "").split("\\.")
      .last.split("(?=[A-Z])").map(_.toLowerCase).mkString("-")
  }

  def conf[T: Table]: TableConfig = implicitly[Table[T]]
}

abstract class Table[T: universe.TypeTag] extends TableConfig {
  type Table = T
  override val tableName: String = Table.defaultTableName[T]
  override val schema: StructType = com.jakainas.functions.schemaFor[T]

  def fullPath: String = {
    def fix(s: String) = s.replaceAll("/+$", "")
    s"${fix(basePath)}/$tableName"
  }
}
