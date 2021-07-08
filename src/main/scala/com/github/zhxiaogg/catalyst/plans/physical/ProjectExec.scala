package com.github.zhxiaogg.catalyst.plans.physical
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, NamedExpression, UnsafeProjection, UnsafeRow}

case class ProjectExec(projectList: Seq[NamedExpression], child: ExecPlan) extends ExecPlan {
  override def output: Seq[Attribute] = projectList.map(_.toAttribute)

  override def children: Seq[ExecPlan] = Seq(child)

  override def execute(context: ExecContext): Seq[InternalRow] = {
    val rows = child.execute(context)
    val projection = UnsafeProjection.create(projectList, child.output)
    val result = rows.map { row =>
      // projection will reuse the returned UnsafeRow, so we need to copy it out
      val r: UnsafeRow = projection(row)
      r.copy()
    }
    result
  }
}
