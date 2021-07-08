package com.github.zhxiaogg.catalyst.plans.physical
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.expressions.Attribute

// TODO: the output should keep the same as logical plan, or the reference will be break
case class ScanExec(table: CatalogTable, override val output: Seq[Attribute]) extends ExecPlan {
  override def execute(context: ExecContext): Seq[InternalRow] = {
    val optTable = context.tables.get(table.identifier.table)
    val rows = optTable.get.rows()
    rows
  }

  override def children: Seq[ExecPlan] = Seq.empty
}
