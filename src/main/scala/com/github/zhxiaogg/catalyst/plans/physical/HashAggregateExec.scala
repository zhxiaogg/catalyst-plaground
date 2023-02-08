package com.github.zhxiaogg.catalyst.plans.physical

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.aggregate.{
  AggregateExpression,
  DeclarativeAggregate,
  ImperativeAggregate
}
import org.apache.spark.sql.catalyst.expressions.{
  Attribute,
  JoinedRow,
  MutableProjection,
  NamedExpression,
  SafeProjection
}

import scala.collection.mutable

case class HashAggregateExec(
  groupings: Seq[NamedExpression],
  aggregates: Seq[AggregateExpression],
  results: Seq[NamedExpression],
  child: ExecPlan
) extends ExecPlan {
  override def execute(context: ExecContext): Seq[InternalRow] = {
    val rows = child.execute(context)

    val groupingProjection = SafeProjection.create(groupings, child.output)
    val groupingAttributes = groupings.map(_.toAttribute)
    val aggregateFunctions = aggregates.map(_.aggregateFunction)

    val aggregatingRows = mutable.Map[InternalRow, InternalRow]()

    val initialValues = aggregateFunctions.flatMap {
      case f: DeclarativeAggregate => f.initialValues
      case f: ImperativeAggregate  => throw new IllegalStateException(s"unsupported function ${f}")
    }
    val initialValueProject = SafeProjection.create(initialValues, child.output)

    val updateExpressions = aggregateFunctions.flatMap {
      case f: DeclarativeAggregate => f.updateExpressions
      case f: ImperativeAggregate  => throw new IllegalStateException(s"unsupported function ${f}")
    }
    val aggUpdateAttributes = aggregateFunctions.flatMap {
      case f: DeclarativeAggregate => f.aggBufferAttributes
      case f: ImperativeAggregate  => throw new IllegalStateException(s"unsupported function ${f}")
    }
    val updateProjection = MutableProjection.create(updateExpressions, aggUpdateAttributes ++ child.output)

    for (row <- rows) {
      val key = groupingProjection.apply(row).copy()
      val aggregations = aggregatingRows.getOrElseUpdate(key, initialValueProject.apply(row).copy())
      updateProjection.target(aggregations)(new JoinedRow(aggregations, row))
    }

    val aggResultAttributes = aggregates.map(_.resultAttribute)
    val aggEvaluateExprs = aggregateFunctions.map {
      case f: DeclarativeAggregate => f.evaluateExpression
      case f: ImperativeAggregate  => throw new IllegalStateException(s"unsupported function ${f}")
    }
    val aggResultProjection = SafeProjection.create(aggEvaluateExprs, aggUpdateAttributes)
    val resultProjection = SafeProjection.create(results, groupingAttributes ++ aggResultAttributes)
    aggregatingRows.map {
      case (groupingKey, aggregations) =>
        resultProjection.apply(new JoinedRow(groupingKey, aggResultProjection.apply(aggregations.copy()))).copy()
    }.toSeq
  }

  override val output: Seq[Attribute] = results.map(_.toAttribute)

  override def children: Seq[ExecPlan] = Seq(child)

  override protected def withNewChildrenInternal(newChildren: IndexedSeq[ExecPlan]): ExecPlan = {
    HashAggregateExec(groupings, aggregates, results, newChildren(0))
  }
}
