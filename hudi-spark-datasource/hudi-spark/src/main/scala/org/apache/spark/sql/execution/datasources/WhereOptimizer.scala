package org.apache.spark.sql.execution.datasources

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.execution.AdbConfig
import org.apache.spark.sql.types.NumericType

/**
 * 生成可衡量的 Condition 列表，用于判断是否值得前置到 prewhere 中执行，衡量的指标主要包括：
 * 1. filter 涉及的列数
 * 2. filter 是否满足一些最优的标准，比如等值查询过滤性较高
 *
 * @param filter      Original filter
 * @param identifiers Columns associated with filter
 * @param priority    Filter priority
 */
case class Condition(filter: Expression,
                     identifiers: Seq[Attribute],
                     priority: Int) {
}

object WhereOptimizer {

  def optimize(filters: Seq[Expression],
               readDataColumn: Seq[Attribute]): Option[Seq[Expression]] = {
    // Filters can be pushed down as prewhere only if it satisfies:
    // 1. not partition filter
    // 2. the referenced column exists
    // 3. has no sub query
    val outputAttrSet = AttributeSet(readDataColumn)
    val selectedFilters = filters
      .filter(_.references.subsetOf(outputAttrSet))
      .filterNot(SubqueryExpression.hasSubquery)

    val conditions = analyze(selectedFilters)
    val prewhereFilters = if (conditions.isEmpty) {
      None
    } else {
      // Sort conditions in ascending order and select top N conditions
      val colNumPercent = SparkSession.getActiveSession.get.conf.get(
        AdbConfig.ADB_PREWHERE_COL_NUM_PERCENT)
      var selectedCol: Seq[Attribute] = Nil
      var selectedFilters: Seq[Expression] = Nil
      conditions
        .sortBy(con => (con.priority, con.identifiers.size))(Ordering.Tuple2(Ordering.Int, Ordering.Int))
        .iterator
        .takeWhile(_ => selectedCol.size.toDouble / readDataColumn.size < colNumPercent)
        .foreach(
          con => {
            selectedFilters ++= Seq(con.filter)
            if (con.identifiers.intersect(selectedCol).isEmpty) {
              selectedCol ++= con.identifiers
            }
          })

      // Pick up conditions which have the selected columns
      conditions
        .filterNot(con => selectedFilters.contains(con.filter))
        .iterator
        .foreach(con => {
          if (selectedCol.containsSlice(con.identifiers)) {
            selectedFilters ++= Seq(con.filter)
          }
        })

      if (selectedFilters.nonEmpty) {
        Some(selectedFilters)
      } else {
        None
      }
    }

    prewhereFilters
  }

  def analyze(filters: Seq[Expression]): Seq[Condition] = {
    val maxInItems = SparkSession.getActiveSession.get.conf.get(
      AdbConfig.ADB_PREWHERE_IN_MAX_NUM)

    filters.flatMap(filter => {
      analyzeImpl(filter, maxInItems)
    })
  }

  def analyzeImpl(filter: Expression, maxInItems: Int): Seq[Condition] = {
    filter match {
      case And(left, right) =>
        analyzeImpl(left, maxInItems) ++ analyzeImpl(right, maxInItems)
      case EqualTo(left, right) =>
        buildCondition(filter, left, right, DefaultPriority.EQUAL_TO)
      case LessThanOrEqual(left, right) =>
        buildCondition(filter, left, right, DefaultPriority.LESS_THAN_OR_EQUAL)
      case LessThan(left, right) =>
        buildCondition(filter, left, right, DefaultPriority.LESS_THAN)
      case GreaterThanOrEqual(left, right) =>
        buildCondition(filter, left, right, DefaultPriority.GREATER_THAN_OR_EQUAL)
      case GreaterThan(left, right) =>
        buildCondition(filter, left, right, DefaultPriority.GREATER_THAN)
      case In(colRef, value) =>
        buildCondition(filter, colRef, value.head, DefaultPriority.IN + value.size)
      case IsNotNull(colRef) =>
        buildCondition(filter, colRef, colRef, DefaultPriority.IS)
      case IsNull(colRef) =>
        buildCondition(filter, colRef, colRef, DefaultPriority.IS)
      case _ =>
        Nil
    }
  }

  def buildCondition(filter: Expression,
                     left: Expression,
                     right: Expression,
                     defaultPriority: Int): Seq[Condition] = {
    val attributes = if (!left.isInstanceOf[Literal]) {
      left.references.toSeq
    } else {
      right.references.toSeq
    }

    val condition = if (attributes.nonEmpty) {
      val isNumericType = attributes.head.dataType.isInstanceOf[NumericType]
      val _priority: Int = if (isNumericType) {
        defaultPriority
      } else {
        defaultPriority + 1
      }

      Seq(Condition(filter, attributes, _priority))
    } else {
      Nil
    }

    condition
  }

  object DefaultPriority {
    val EQUAL_TO = 1
    val LESS_THAN_OR_EQUAL = 20
    val LESS_THAN = 30
    val GREATER_THAN_OR_EQUAL = 40
    val GREATER_THAN = 50
    val RANGE = 60
    val IN = 70
    val IS = 80

    val UN_KNOWN = 9999
  }
}


