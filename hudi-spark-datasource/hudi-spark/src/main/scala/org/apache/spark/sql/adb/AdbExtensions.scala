package org.apache.spark.sql.adb

import org.apache.spark.sql.SparkSessionExtensions
import org.apache.spark.sql.execution.datasources.AdbFileSourceStrategy


class AdbExtensions extends (SparkSessionExtensions => Unit) {
  override def apply(extensions: SparkSessionExtensions): Unit = {
    // Adb Custom Strategy.
    extensions.injectPlannerStrategy(_ => AdbFileSourceStrategy)
    // Adb Custom SqlParser.
    //    extensions.injectParser((session, delegate) => {
    //      new AdbSparkSqlParser(session, delegate)
    //    })
  }
}
