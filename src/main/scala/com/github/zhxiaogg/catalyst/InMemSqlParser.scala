package com.github.zhxiaogg.catalyst

import org.apache.spark.sql.catalyst.parser.{AbstractSqlParser, AstBuilder}
import org.apache.spark.sql.internal.SQLConf

class InMemSqlParser extends AbstractSqlParser{
  override protected def astBuilder: AstBuilder = new AstBuilder

  /** now we need SPARK_LOCAL_HOSTNAME to speed up the initialization process */
  override def conf: SQLConf = super.conf
}
