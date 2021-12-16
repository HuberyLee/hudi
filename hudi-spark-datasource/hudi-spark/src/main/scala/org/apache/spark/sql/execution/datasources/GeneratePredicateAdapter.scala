package org.apache.spark.sql.execution.datasources

import org.apache.spark.sql.catalyst.expressions
import org.apache.spark.sql.catalyst.expressions.codegen.{GeneratePredicate, Predicate}
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression}

class GeneratePredicateAdapter(filters: Seq[Expression], output: Seq[Attribute]) {
  def generate(): Predicate = {
    GeneratePredicate.generate(filters.reduce(expressions.And), output)
  }
}
