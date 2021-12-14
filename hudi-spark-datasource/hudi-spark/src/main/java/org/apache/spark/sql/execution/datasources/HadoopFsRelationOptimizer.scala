package org.apache.spark.sql.execution.datasources

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.execution.datasources.parquet.{OptimizedParquetFileFormat, ParquetFileFormat}
import org.apache.spark.sql.internal.hudi.HoodieConf
import org.apache.spark.sql.types.StructType

object HadoopFsRelationOptimizer extends Logging {

  /**
   * Return (HadoopFsRelation, Boolean) Tuple,
   * if use prewhere, return (OptimizedRelation, true)
   * else (OriginalRelation, false).
   */
  def tryOptimize(
                   relation: HadoopFsRelation,
                   partitionKeyFilters: Seq[Expression],
                   dataFilters: Seq[Expression],
                   outputSchema: StructType): (HadoopFsRelation, Boolean) = {
    val parquetEnabled = relation.sparkSession.conf.get(HoodieConf.ADB_PARQUET_ENABLED)

    relation.fileFormat match {
      case _: ParquetFileFormat if parquetEnabled =>
        val optimizedParquetFileFormat = new OptimizedParquetFileFormat

        logInfo("using optimized parquet file format.")
        (relation.copy(fileFormat = optimizedParquetFileFormat)(relation.sparkSession), true)

      case _: FileFormat =>
        (relation, false)
    }
  }
}
