package scromium.api

import org.apache.cassandra.thrift
import scromium.serializers._
import scala.collection.JavaConversions._

class GetSuperColumn(superColumn : thrift.SuperColumn) extends Iterable[GetColumn] {
  def this(result : thrift.ColumnOrSuperColumn) {
    this(result.super_column)
  }
  var columns = superColumn.columns.map(new GetColumn(_))
  
  def name = superColumn.name
  def nameAs[T](deserializer : Deserializer[T]) = deserializer.deserialize(name)
  def iterator = columns.iterator
}