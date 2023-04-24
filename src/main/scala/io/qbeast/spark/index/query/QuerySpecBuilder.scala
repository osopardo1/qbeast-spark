/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.index.query

import io.qbeast.core.model._
import io.qbeast.spark.internal.expressions.QbeastMurmur3Hash
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.analysis.Resolver
import org.apache.spark.sql.catalyst.expressions.{
  And,
  BinaryComparison,
  EqualTo,
  Expression,
  GreaterThan,
  GreaterThanOrEqual,
  IsNull,
  LessThan,
  LessThanOrEqual,
  Literal,
  Or,
  SubqueryExpression
}
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.unsafe.types.UTF8String

/**
 * Builds a query specification from a set of filters
 * @param sparkFilters the filters
 */
private[spark] class QuerySpecBuilder(sparkFilters: Seq[Expression])
    extends Serializable
    with StagingUtils {

  lazy val spark: SparkSession = SparkSession.active
  lazy val nameEquality: Resolver = spark.sessionState.analyzer.resolver

  private def hasQbeastColumnReference(expr: Expression, indexedColumns: Seq[String]): Boolean = {
    expr.references.forall { r =>
      indexedColumns.exists(nameEquality(r.name, _))
    }
  }

  private def isQbeastWeightExpression(expression: Expression): Boolean = {
    expression match {
      case BinaryComparison(_: QbeastMurmur3Hash, _) => true
      case _ => false
    }
  }

  private def isDisjunctivePredicate(condition: Expression): Boolean = {
    condition.isInstanceOf[Or]
  }

  private def splitDisjunctivePredicates(condition: Expression): Seq[Expression] = {
    condition match {
      case Or(cond1, cond2) =>
        splitDisjunctivePredicates(cond1) ++ splitDisjunctivePredicates(cond2)
      case other => other :: Nil
    }
  }

  private def splitConjunctivePredicates(condition: Expression): Seq[Expression] = {
    condition match {
      case And(cond1, cond2) =>
        splitConjunctivePredicates(cond1) ++ splitConjunctivePredicates(cond2)
      case other => other :: Nil
    }
  }

  private def hasColumnReference(expr: Expression, columnName: String): Boolean = {
    expr.references.forall(r => nameEquality(r.name, columnName))
  }

  /**
   * Extracts the data filters from the query that can be used by qbeast
   * @param dataFilters filters passed to the relation
   * @param revision the revision of the index
   * @return sequence of filters involving qbeast format
   */
  def extractQbeastFilters(dataFilters: Seq[Expression], revision: Revision): QbeastFilters = {
    val indexedColumns = revision.columnTransformers.map(_.columnName)
    val notSubqueryFilters =
      dataFilters.filter(!SubqueryExpression.hasSubquery(_)).flatMap(splitConjunctivePredicates)
    val weightFilters = notSubqueryFilters.filter(isQbeastWeightExpression)
    val queryFilters = notSubqueryFilters.filter(hasQbeastColumnReference(_, indexedColumns))

    QbeastFilters(weightFilters, queryFilters)
  }

  private def sparkTypeToCoreType(value: Any): Any = {
    value match {
      case s: UTF8String => s.toString
      case _ => value
    }
  }

  /**
   * Extracts the sequence of query spaces
   * That we should union after
   * @param rangePredicate the predicates passed by the spark engine
   * @param revision the characteristics of the index
   * @return
   */

  def extractQuerySpace(rangePredicate: Seq[Expression], revision: Revision): QuerySpace = {

    // Data Filters should not be empty
    assert(rangePredicate.nonEmpty)

    val indexedColumns = revision.columnTransformers.map(_.columnName)

    val (from, to) =
      indexedColumns.map { columnName =>
        // Get the filters related to the column
        val columnFilters = rangePredicate.filter(hasColumnReference(_, columnName))

        // Get the coordinates of the column in the filters,
        // if not found, use the overall coordinates
        val columnFrom = columnFilters
          .collectFirst {
            case GreaterThan(_, Literal(value, _)) => sparkTypeToCoreType(value)
            case GreaterThanOrEqual(_, Literal(value, _)) => sparkTypeToCoreType(value)
            case LessThan(Literal(value, _), _) => sparkTypeToCoreType(value)
            case LessThanOrEqual(Literal(value, _), _) => sparkTypeToCoreType(value)
            case EqualTo(_, Literal(value, _)) => sparkTypeToCoreType(value)
            case EqualTo(Literal(value, _), _) => sparkTypeToCoreType(value)
            case IsNull(_) => null
          }

        val columnTo = columnFilters
          .collectFirst {
            case LessThan(_, Literal(value, _)) => sparkTypeToCoreType(value)
            case LessThanOrEqual(_, Literal(value, _)) => sparkTypeToCoreType(value)
            case GreaterThan(Literal(value, _), _) => sparkTypeToCoreType(value)
            case GreaterThanOrEqual(Literal(value, _), _) => sparkTypeToCoreType(value)
            case EqualTo(_, Literal(value, _)) => sparkTypeToCoreType(value)
            case EqualTo(Literal(value, _), _) => sparkTypeToCoreType(value)
            case IsNull(_) => null
          }

        (columnFrom, columnTo)
      }.unzip

    QuerySpace(from, to, revision.transformations)

  }

  /**
   * Extracts the sampling weight range of the query
   * @param dataFilters the filters passed by the spark engine
   * @return the upper and lower weight bounds (default: Weight.MinValue, Weight.MaxValue)
   */
  def extractWeightRange(dataFilters: Seq[Expression]): WeightRange = {

    val weightFilters = dataFilters.filter(isQbeastWeightExpression)

    val min = weightFilters
      .collectFirst { case GreaterThanOrEqual(_, Literal(m, IntegerType)) =>
        m.asInstanceOf[Int]
      }
      .getOrElse(Int.MinValue)

    val max = weightFilters
      .collectFirst { case LessThan(_, Literal(m, IntegerType)) =>
        m.asInstanceOf[Int]
      }
      .getOrElse(Int.MaxValue)

    WeightRange(Weight(min), Weight(max))
  }

  /**
   * Builds the QuerySpec for the desired Revision
   * @param revision the revision
   * @return the QuerySpec
   */

  def build(revision: Revision): Seq[QuerySpec] = {

    val qbeastFilters = extractQbeastFilters(sparkFilters, revision)
    val weightRange = extractWeightRange(qbeastFilters.weightFilters)

    if (isStaging(revision)) {
      Seq(QuerySpec(WeightRange(Weight(Int.MinValue), Weight(Int.MaxValue)), EmptySpace()))
    } else if (qbeastFilters.queryFilters.isEmpty) {
      Seq(QuerySpec(weightRange, AllSpace()))
    } else {

      // First split disjunctive predicates
      // To generate a QuerySpec for each space
      val (disjunctivePredicates, conjunctivePredicates) =
        qbeastFilters.queryFilters.partition(isDisjunctivePredicate)

      // Process the conjunctive predicates, if any
      val processedPredicates =
        if (conjunctivePredicates.isEmpty) Seq.empty
        else Seq(QuerySpec(weightRange, extractQuerySpace(conjunctivePredicates, revision)))

      // Process each disjunctive predicate as a different QuerySpec
      val processedDisjunctivePredicates = disjunctivePredicates
        .flatMap(splitDisjunctivePredicates)
        .map(f => {
          QuerySpec(weightRange, extractQuerySpace(splitConjunctivePredicates(f), revision))
        })

      // Add both sets of predicates to the final query
      processedDisjunctivePredicates ++ processedPredicates
    }
  }

}
