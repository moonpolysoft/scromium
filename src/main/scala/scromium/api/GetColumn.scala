package scromium.api

import scromium.serializers.Serializer
import org.apache.cassandra.thrift

/**
 * Represents the result of a single column get operation.
 */
class GetColumn(column : thrift.Column) {
  def this(result : thrift.ColumnOrSuperColumn) {
    this(result.column)
  }
  
  def name = column.name
  def value = column.value
  def timestamp = column.timestamp
  
  def valueAs[T](implicit serializer : Serializer[T]) = serializer.deserialize(value)
  def nameAs[T](implicit serializer : Serializer[T]) = serializer.deserialize(name)
}