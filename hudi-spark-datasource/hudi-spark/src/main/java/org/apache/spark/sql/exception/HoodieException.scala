package org.apache.spark.sql.exception

class HoodieException(message: String, cause: Throwable) extends Exception(message, cause) {
  def this(message: String) = this(message, null)
}
