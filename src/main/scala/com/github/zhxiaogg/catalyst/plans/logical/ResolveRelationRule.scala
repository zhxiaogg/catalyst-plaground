package com.github.zhxiaogg.catalyst.plans.logical

import org.apache.spark.sql.catalyst.QualifiedTableName
import org.apache.spark.sql.catalyst.catalog.{SessionCatalog, UnresolvedCatalogRelation}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule

class ResolveRelationRule(sessionCatalog: SessionCatalog) extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan = plan.transformUp {
    case UnresolvedCatalogRelation(table, options, isStreaming) =>
      val qualifiedTableName = QualifiedTableName(table.database, table.identifier.table)
      sessionCatalog.getCachedPlan(qualifiedTableName, () => {
        LogicalRelation(table)
      })
    // LogicalRelation(table)
  }
}
