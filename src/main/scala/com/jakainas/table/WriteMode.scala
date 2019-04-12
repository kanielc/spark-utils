package com.jakainas.table

object WriteMode {
  val PART_OVERWRITE = WriteMode("part_overwrite")
}

case class WriteMode(name: String) extends AnyVal