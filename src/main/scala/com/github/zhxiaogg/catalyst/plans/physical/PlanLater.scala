package com.github.zhxiaogg.catalyst.plans.physical

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

case class PlanLater(plan: LogicalPlan) extends ExecPlan {
  override def execute(context: ExecContext): Seq[InternalRow] = throw new IllegalStateException("unsupported!")

  override def output: Seq[Attribute] = plan.output

  override def children: Seq[ExecPlan] = Seq()

  override protected def withNewChildrenInternal(newChildren: IndexedSeq[ExecPlan]): ExecPlan =
    PlanLater(plan)
}
