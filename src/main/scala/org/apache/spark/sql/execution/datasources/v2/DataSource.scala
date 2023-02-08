package org.apache.spark.sql.execution.datasources.v2

import org.apache.spark.sql.catalyst.catalog.{CatalogStorageFormat, CatalogUtils}
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap

import java.util.Locale

object DataSource {
  /**
    * When creating a data source table, the `path` option has a special meaning: the table location.
    * This method extracts the `path` option and treat it as table location to build a
    * [[CatalogStorageFormat]]. Note that, the `path` option is removed from options after this.
    */
  def buildStorageFormatFromOptions(options: Map[String, String]): CatalogStorageFormat = {
    val path = CaseInsensitiveMap(options).get("path")
    val optionsWithoutPath = options.filterKeys(_.toLowerCase(Locale.ROOT) != "path")
    CatalogStorageFormat.empty.copy(
      locationUri = path.map(CatalogUtils.stringToURI), properties = optionsWithoutPath.toMap)
  }
}
