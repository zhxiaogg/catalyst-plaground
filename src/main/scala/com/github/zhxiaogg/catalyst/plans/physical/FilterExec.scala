package com.github.zhxiaogg.catalyst.plans.physical

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression, Predicate}

case class FilterExec(condition: Expression, child: ExecPlan) extends ExecPlan {
  override def execute(context: ExecContext): Seq[InternalRow] = {
    val rows = child.execute(context)
    val predicate = Predicate.create(condition, child.output)
    val result = rows.filter(row => predicate.eval(row))
    result
  }

  override def output: Seq[Attribute] = child.output

  override def children: Seq[ExecPlan] = Seq(child)

  override protected def withNewChildrenInternal(newChildren: IndexedSeq[ExecPlan]): ExecPlan = {
    FilterExec(condition, newChildren(0))
  }
}
