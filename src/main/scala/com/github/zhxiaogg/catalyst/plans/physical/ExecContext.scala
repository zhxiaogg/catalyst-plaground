package com.github.zhxiaogg.catalyst.plans.physical

import com.github.zhxiaogg.catalyst.plans.physical.ExecContext.ObjectTable
import org.apache.spark.sql.catalyst.{InternalRow, TableIdentifier}
import org.apache.spark.sql.catalyst.catalog.{CatalogStorageFormat, CatalogTable, CatalogTableType, SessionCatalog}
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.types._

import scala.reflect.ClassTag

object ExecContext {
  case class ObjectTable[T](name: String, var values: Seq[T])(implicit clazz: ClassTag[T]) {
    def schema: StructType = {
      val fields = clazz.runtimeClass.getDeclaredFields.map(f => StructField(f.getName, dataTypeOf(f.getType)))
      StructType(fields)
    }

    private val StringClass = classOf[String]
    private val IntClass = classOf[Int]
    private val LongClass = classOf[Long]
    def dataTypeOf(clazz: Class[_]): DataType = clazz match {
      case StringClass => StringType
      case IntClass    => IntegerType
      case LongClass   => LongType
      case x           => throw new UnsupportedOperationException(x.getCanonicalName)
    }

    def rows(): Seq[InternalRow] = {
      val fields = schema.fields.map { f =>
        val field = clazz.runtimeClass.getDeclaredField(f.name)
        field.setAccessible(true)
        field
      }
      values.map(v => fields.map(f => f.get(v).asInstanceOf[Any])).map(cols => new GenericInternalRow(cols))
    }
  }
}

case class ExecContext(tables: Map[String, ObjectTable[_]]) {
  def init(sessionCatalog: SessionCatalog): Unit = {
    tables.values.foreach { t =>
      val table = CatalogTable(
        identifier = TableIdentifier(t.name, Some("default")), // TODO: make database configurable
        tableType = CatalogTableType.MANAGED,
        storage = CatalogStorageFormat.empty,
        schema = t.schema,
        provider = None,
        partitionColumnNames = Seq()
      )
      sessionCatalog.createTable(table, ignoreIfExists = false)
    }
  }

}
