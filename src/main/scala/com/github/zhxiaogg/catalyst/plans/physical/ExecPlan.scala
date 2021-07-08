package com.github.zhxiaogg.catalyst.plans.physical

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.plans.QueryPlan

abstract class ExecPlan extends QueryPlan[ExecPlan] {
  def execute(context: ExecContext): Seq[InternalRow]
}
