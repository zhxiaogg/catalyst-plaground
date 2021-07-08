package org.apache.spark.sql.connector.catalog

import org.apache.spark.sql.catalyst.catalog.SessionCatalog

object CatalogManagerUtil {
  def create(catalog: SessionCatalog) = new CatalogManager(new V2SessionCatalog(catalog), catalog)
}
