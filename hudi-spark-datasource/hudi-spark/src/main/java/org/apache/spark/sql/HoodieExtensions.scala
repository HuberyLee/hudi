package org.apache.spark.sql

import org.apache.spark.sql.execution.datasources.HoodieFileSourceStrategy


class HoodieExtensions extends (SparkSessionExtensions => Unit) {
  override def apply(extensions: SparkSessionExtensions): Unit = {
    // Adb Custom Strategy.
    extensions.injectPlannerStrategy(_ => HoodieFileSourceStrategy)
    // Adb Custom SqlParser.
    //    extensions.injectParser((session, delegate) => {
    //      new AdbSparkSqlParser(session, delegate)
    //    })
  }
}
