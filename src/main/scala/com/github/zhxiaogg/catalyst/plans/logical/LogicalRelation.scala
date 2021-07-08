package com.github.zhxiaogg.catalyst.plans.logical

import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.LeafNode
import org.apache.spark.sql.utils.StructTypeExt

case class LogicalRelation(table: CatalogTable) extends LeafNode {
  override val output: Seq[Attribute] = table.schema.asAttributes
}
