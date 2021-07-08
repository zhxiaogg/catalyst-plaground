package org.apache.spark.sql

import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.types.StructType

package object utils {
  implicit class StructTypeExt(structType: StructType) {
    def asAttributes: Seq[AttributeReference] = structType.toAttributes
  }
}
