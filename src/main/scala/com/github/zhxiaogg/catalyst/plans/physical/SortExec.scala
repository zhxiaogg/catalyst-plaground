package com.github.zhxiaogg.catalyst.plans.physical

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, SortOrder}

case class SortExec(order: Seq[SortOrder], plan: ExecPlan) extends ExecPlan {
  override def execute(context: ExecContext): Seq[InternalRow] = {
    // TODO: implement sort
    val rows = plan.execute(context)
    rows
  }

  override def output: Seq[Attribute] = plan.output

  override def children: Seq[ExecPlan] = Seq(plan)

  override protected def withNewChildrenInternal(newChildren: IndexedSeq[ExecPlan]): ExecPlan = {
    SortExec(order, newChildren(0))
  }
}
