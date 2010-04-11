package scromium.api

import org.apache.cassandra.thrift
import scromium.serializers._
import scala.collection.JavaConversions._

case class GetSuperColumn(val name : Array[Byte], val columns : Seq[GetColumn]) extends Iterable[GetColumn] with Container {
  def this(superColumn : thrift.SuperColumn) {
    this(superColumn.name, superColumn.columns.map(new GetColumn(_)))
  }
  
  def this(result : thrift.ColumnOrSuperColumn) {
    this(result.super_column)
  }
  
  def nameAs[T](implicit deserializer : Deserializer[T]) = deserializer.deserialize(name)
  def iterator = columns.iterator
}