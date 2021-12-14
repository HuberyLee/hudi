package org.apache.spark.sql.internal.hudi

import org.apache.spark.internal.config.ConfigEntry
import org.apache.spark.sql.internal.SQLConf

object HoodieConf {
  val ADB_PREWHERE_ENABLED: ConfigEntry[Boolean] = SQLConf.buildConf("spark.sql.adb.prewhere.enabled")
    .internal()
    .doc("Whether enable prewhere when query hoodie table")
    .booleanConf
    .createWithDefault(true)

  val ADB_PARQUET_ENABLED: ConfigEntry[Boolean] = SQLConf.buildConf("spark.sql.adb.parquet.enabled")
    .internal()
    .doc("Whether enable prewhere for parquet when query hoodie table")
    .booleanConf
    .createWithDefault(true)

  val ADB_ORC_ENABLED: ConfigEntry[Boolean] = SQLConf.buildConf("spark.sql.adb.orc.enabled")
    .internal()
    .doc("Whether enable prewhere for orc when query hoodie table")
    .booleanConf
    .createWithDefault(false)
}
