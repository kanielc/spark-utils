package com.jakainas.table

trait DailyPartitioning extends TableConfig {
  override val partitioning = Seq("year", "month", "day")
}
