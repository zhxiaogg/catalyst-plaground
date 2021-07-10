package com.github.zhxiaogg.catalyst

import com.github.zhxiaogg.catalyst.plans.logical.ResolveRelationRule
import com.github.zhxiaogg.catalyst.plans.physical.ExecContext
import com.github.zhxiaogg.catalyst.plans.physical.ExecContext.ObjectsRelation
import org.apache.spark.sql.catalyst.analysis.{Analyzer, EmptyFunctionRegistry, FunctionRegistry}
import org.apache.spark.sql.catalyst.catalog._
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.connector.catalog.CatalogManagerUtil

import java.net.URI

object Playground {

  case class Movie(id: Int, name: String, tvt: Int, clicks: Int)

  val database: CatalogDatabase = new CatalogDatabase("default", "default", URI.create("/tmp/"), Map.empty)

  def main(args: Array[String]): Unit = {
    val analyzer = createAnalyser()

    val movies = ObjectsRelation[Movie]("movies", Seq(Movie(1, "action", 10, 10), Movie(7, "comedy", 10, 10)))
    println(movies.schema)
    val context = ExecContext(Map("movies" -> movies))
    context.init(analyzer.catalogManager.v1SessionCatalog)

    val plan: LogicalPlan = CatalystSqlParser.parsePlan("select id, name from movies where id >= 1")
    println(plan)
    val resolved = analyzer.execute(plan)
    println(resolved)

    val planner = new InMemQueryPlanner
    val exec = planner.plan(resolved).next()
    println(exec)

    // this context is important here, it means we can pass in different tables when executing the plan
    val rows = exec.execute(context)
    println(rows)
  }

  private def createAnalyser(): Analyzer = {
    val database: CatalogDatabase = new CatalogDatabase("default", "default", URI.create("/tmp/"), Map.empty)

    val catalog = new SessionCatalog(new InMemoryCatalog, FunctionRegistry.builtin)
    lazy val catalogManager = CatalogManagerUtil.create(catalog)

    val analyzer = new Analyzer(catalogManager) {
      override val extendedResolutionRules: Seq[Rule[LogicalPlan]] = Seq(new ResolveRelationRule(catalog))
    }

    analyzer.catalogManager.v1SessionCatalog.createDatabase(database, ignoreIfExists = true)
    analyzer
  }

}
