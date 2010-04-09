package scromium.api

import scromium.serializers.Deserializer
import org.apache.cassandra.thrift

/**
 * Represents the result of a single column get operation.
 */
case class GetColumn(val name : Array[Byte], val value : Array[Byte], val timestamp : Long) extends Container {
  def this(column : thrift.Column) {
    this(column.name, column.value, column.timestamp)
  }
  
  def this(result : thrift.ColumnOrSuperColumn) {
    this(result.column)
  }
  
  def valueAs[T](implicit des : Deserializer[T]) = des.deserialize(value)
  def nameAs[T](implicit des : Deserializer[T]) = des.deserialize(name)
}