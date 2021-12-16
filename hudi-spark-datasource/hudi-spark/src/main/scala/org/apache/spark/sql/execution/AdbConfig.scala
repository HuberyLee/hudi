package org.apache.spark.sql.execution

import org.apache.spark.internal.config.ConfigEntry
import org.apache.spark.sql.internal.SQLConf

object AdbConfig {
  val ADB_WHERE_OPTIMIZATION_ENABLED: ConfigEntry[Boolean] = SQLConf
    .buildConf("spark.sql.adb.where.optimization.enabled")
    .internal()
    .doc("Whether enable where optimization when query hoodie table")
    .booleanConf
    .createWithDefault(true)

  val ADB_PARQUET_ENABLED: ConfigEntry[Boolean] = SQLConf
    .buildConf("spark.sql.adb.parquet.enabled")
    .internal()
    .doc("Whether enable prewhere for parquet when query hoodie table")
    .booleanConf
    .createWithDefault(true)

  val ADB_ORC_ENABLED: ConfigEntry[Boolean] = SQLConf
    .buildConf("spark.sql.adb.orc.enabled")
    .internal()
    .doc("Whether enable prewhere for orc when query hoodie table")
    .booleanConf
    .createWithDefault(false)

  val ADB_PREWHERE_IN_MAX_NUM: ConfigEntry[Int] = SQLConf
    .buildConf("spark.sql.adb.prewhere.in.max.num")
    .internal()
    .doc("Max num of in's item when move to prewhere")
    .intConf
    .createWithDefault(10)

  val ADB_PREWHERE_COL_NUM_PERCENT: ConfigEntry[Double] = SQLConf
    .buildConf("spark.sql.adb.prewhere.col.max.num.percent")
    .internal()
    .doc("Max column num percent of prewhere predicates")
    .doubleConf
    .createWithDefault(1.0)
}
