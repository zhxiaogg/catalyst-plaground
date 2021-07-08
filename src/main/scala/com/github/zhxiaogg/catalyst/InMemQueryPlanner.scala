package com.github.zhxiaogg.catalyst

import com.github.zhxiaogg.catalyst.plans.physical.{ExecPlan, PlanLater, SimpleExecStrategy}
import org.apache.spark.sql.catalyst.planning.{GenericStrategy, QueryPlanner}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

class InMemQueryPlanner extends QueryPlanner[ExecPlan] {
  override def strategies: Seq[GenericStrategy[ExecPlan]] =
    Seq(new SimpleExecStrategy)

  override protected def collectPlaceholders(plan: ExecPlan): Seq[(ExecPlan, LogicalPlan)] = plan.collect {
    case placeholder @ PlanLater(logicalPlan) => placeholder -> logicalPlan
  }

  override protected def prunePlans(plans: Iterator[ExecPlan]): Iterator[ExecPlan] = plans
}
