package org.apache.spark.sql.connector.catalog

import org.apache.spark.sql.catalyst.catalog.SessionCatalog
import org.apache.spark.sql.execution.datasources.v2.V2SessionCatalog

object CatalogManagerUtil {
  def create(catalog: SessionCatalog) = new CatalogManager(new V2SessionCatalog(catalog), catalog)
}
