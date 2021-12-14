package org.apache.spark.sql.execution.datasources

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions.PredicateHelper
import org.apache.spark.sql.catalyst.planning.PhysicalOperation
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.exception.HoodieException
import org.apache.spark.sql.execution._
import org.apache.spark.sql.internal.hudi.HoodieConf
import org.apache.spark.sql.{SparkSession, Strategy, execution}

/**
 * AdbFileSourceStrategy use to intercept [[FileSourceStrategy]]
 */
object HoodieFileSourceStrategy extends Strategy with PredicateHelper with Logging {

  def apply(plan: LogicalPlan): Seq[SparkPlan] = {
    /**
     * After call [[FileSourceStrategy]], the result [[SparkPlan]] should the follow 4 scenarios:
     *  1. [[ProjectExec]] -> [[FilterExec]] -> [[FileSourceScanExec]]
     *     2. [[ProjectExec]] -> [[FileSourceScanExec]]
     *     3. [[FilterExec]] -> [[FileSourceScanExec]]
     *     4. [[FileSourceScanExec]]
     *     Classified discussion the 4 scenarios and assemble a new [[SparkPlan]] if can optimized.
     */
    def tryOptimize(head: SparkPlan): SparkPlan = {
      val prewhereEnable = SparkSession.getActiveSession.get.conf.get(HoodieConf.ADB_PREWHERE_ENABLED)
      if (!prewhereEnable) {
        logInfo("Prewhere is disabled in runtime," +
          " will fall back to default Parquet/ORC file format.")
        return head
      }

      head match {
        // ProjectExec -> FilterExec -> FileSourceScanExec
        case ProjectExec(projectList, FilterExec(condition,
        FileSourceScanExec(relation, output, outputSchema, partitionFilters, optionalBucketSet,
        dataFilters, tableIdentifier))) =>

          val (hadoopFsRelation, isOptimized) = HadoopFsRelationOptimizer.tryOptimize(
            relation, partitionFilters, dataFilters, outputSchema)
          if (isOptimized) {
            val scan = HoodieFileSourceScanExec(hadoopFsRelation, output, outputSchema,
              partitionFilters, optionalBucketSet, dataFilters, tableIdentifier)
            execution.ProjectExec(projectList, execution.FilterExec(condition, scan))
          } else {
            head
          }
        // ProjectExec -> FileSourceScanExec
        case ProjectExec(projectList,
        FileSourceScanExec(relation, output, outputSchema, partitionFilters, optionalBucketSet,
        dataFilters, tableIdentifier)) =>

          val (hadoopFsRelation, isOptimized) = HadoopFsRelationOptimizer.tryOptimize(
            relation, partitionFilters, dataFilters, outputSchema)
          if (isOptimized) {
            val scan = HoodieFileSourceScanExec(hadoopFsRelation, output, outputSchema,
              partitionFilters, optionalBucketSet, dataFilters, tableIdentifier)
            execution.ProjectExec(projectList, scan)
          } else {
            head
          }
        // FilterExec -> FileSourceScanExec
        case FilterExec(condition, FileSourceScanExec(relation, output, outputSchema,
        partitionFilters, optionalBucketSet, dataFilters, tableIdentifier)) =>

          val (hadoopFsRelation, isOptimized) = HadoopFsRelationOptimizer.tryOptimize(
            relation, partitionFilters, dataFilters, outputSchema)
          if (isOptimized) {
            val scan = HoodieFileSourceScanExec(hadoopFsRelation, output, outputSchema,
              partitionFilters, optionalBucketSet, dataFilters, tableIdentifier)
            execution.FilterExec(condition, scan)
          } else {
            head
          }
        // FileSourceScanExec
        case FileSourceScanExec(relation, output, outputSchema, partitionFilters, optionalBucketSet,
        dataFilters, tableIdentifier) =>

          val (hadoopFsRelation, isOptimized) = HadoopFsRelationOptimizer.tryOptimize(
            relation, partitionFilters, dataFilters, outputSchema)
          if (isOptimized) {
            HoodieFileSourceScanExec(hadoopFsRelation, output, outputSchema,
              partitionFilters, optionalBucketSet, dataFilters, tableIdentifier)
          } else {
            head
          }
        case _ => throw new HoodieException(s"Unsupport plan mode $head")
      }
    }

    plan match {
      case PhysicalOperation(_, _, LogicalRelation(_: HadoopFsRelation, _, _, _)) =>
        FileSourceStrategy(plan).headOption match {
          case Some(head) => tryOptimize(head) :: Nil
          case _ => Nil
        }
      case _ => Nil
    }
  }
}
