package com.github.zhxiaogg.catalyst.plans.physical

import com.github.zhxiaogg.catalyst.plans.physical.ExecContext.ObjectsRelation
import org.apache.spark.sql.catalyst.{InternalRow, TableIdentifier}
import org.apache.spark.sql.catalyst.catalog.{CatalogStorageFormat, CatalogTable, CatalogTableType, SessionCatalog}
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

import scala.reflect.ClassTag

object ExecContext {

  private val StringClass = classOf[String]
  private val IntClass = classOf[Int]
  private val LongClass = classOf[Long]

  /** A relation for Scala/Java objects. */
  case class ObjectsRelation[T](name: String, var values: Seq[T])(implicit clazz: ClassTag[T]) {
    private lazy val reader: T => InternalRow = {
      val readers = schema.fields.map { f =>
        val field = clazz.runtimeClass.getDeclaredField(f.name)
        field.setAccessible(true)
        field.getType match {
          case StringClass => o: Object => UTF8String.fromString(field.get(o).asInstanceOf[String])
          case _           => o: Object => field.get(o).asInstanceOf[Any]
        }
      }
      t: T => new GenericInternalRow(readers.map(f => f.apply(t.asInstanceOf[Object])))
    }

    def schema: StructType = {
      val fields = clazz.runtimeClass.getDeclaredFields.map(f => StructField(f.getName, dataTypeOf(f.getType)))
      StructType(fields)
    }

    def dataTypeOf(clazz: Class[_]): DataType = clazz match {
      case StringClass => StringType
      case IntClass    => IntegerType
      case LongClass   => LongType
      case x           => throw new UnsupportedOperationException(x.getCanonicalName)
    }

    def rows(): Seq[InternalRow] = values.map(v => reader(v))
  }
}

case class ExecContext(tables: Map[String, ObjectsRelation[_]]) {
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
