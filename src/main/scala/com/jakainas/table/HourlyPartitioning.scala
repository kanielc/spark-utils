package com.jakainas.table

trait HourlyPartitioning extends TableConfig {
  override val partitioning = Seq("year", "month", "day", "hour")
}
