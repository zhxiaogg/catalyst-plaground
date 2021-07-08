package com.github.zhxiaogg.catalyst

import com.github.zhxiaogg.catalyst.plans.logical.ResolveRelationRule
import com.github.zhxiaogg.catalyst.plans.physical.ExecContext
import com.github.zhxiaogg.catalyst.plans.physical.ExecContext.ObjectTable
import org.apache.spark.sql.catalyst.analysis.{Analyzer, EmptyFunctionRegistry}
import org.apache.spark.sql.catalyst.catalog._
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.connector.catalog.CatalogManagerUtil

import java.net.URI

object Playground {

  // TODO: String is not supported yet
  case class User(id: Int, name: Long, age: Int)

  val database: CatalogDatabase = new CatalogDatabase("default", "default", URI.create("/tmp/"), Map.empty)

  def main(args: Array[String]): Unit = {
    val plan: LogicalPlan = CatalystSqlParser.parsePlan("select id, name from users where id >= 1")
    val inMemCatalog = new InMemoryCatalog
    val catalog = new SessionCatalog(inMemCatalog, EmptyFunctionRegistry)
    lazy val catalogManager = CatalogManagerUtil.create(catalog)

    val analyzer = new Analyzer(catalogManager) {
      override val extendedResolutionRules: Seq[Rule[LogicalPlan]] = Seq(new ResolveRelationRule(catalog))
    }

    analyzer.catalogManager.v1SessionCatalog.createDatabase(database, ignoreIfExists = true)

    val users = ObjectTable[User]("users", Seq(User(1, 2L, 10), User(7, 9L, 10)))
    println(users.schema)
    val context = ExecContext(Map("users" -> users))
    context.init(analyzer.catalogManager.v1SessionCatalog)

    println(plan)
    val resolved = analyzer.execute(plan)
    println(resolved)

    val planner = new InMemQueryPlanner
    val exec = planner.plan(resolved).next()
    println(exec)

    // this context is important here, it means we can pass in different tables when executing the plan
    val rows = exec.execute(context)
    println(rows)

    //val plan2: LogicalPlan = CatalystSqlParser.parsePlan("select id, sum(wage) from users group by id having sum(wage) > 10")

  }
}
