package com.github.zhxiaogg.catalyst.plans.physical

import com.github.zhxiaogg.catalyst.plans.logical.LogicalRelation
import org.apache.spark.sql.catalyst.planning.GenericStrategy
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LogicalPlan, Project, SubqueryAlias}

class SimpleExecStrategy extends GenericStrategy[ExecPlan] {
  override protected def planLater(plan: LogicalPlan): ExecPlan = PlanLater(plan)

  override def apply(plan: LogicalPlan): Seq[ExecPlan] = plan match {
    case Project(projectList, child)      => Seq(ProjectExec(projectList, planLater(child)))
    case Filter(condition, child)         => Seq(FilterExec(condition, planLater(child)))
    case SubqueryAlias(identifier, child) => Seq(planLater(child)) // TODO: we don't use SubqueryAlias for now
    case r @ LogicalRelation(table)       => Seq(ScanExec(table, r.output))
  }
}
