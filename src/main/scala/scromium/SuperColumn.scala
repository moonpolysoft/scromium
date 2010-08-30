package scromium

import serializers._
import scala.collection.JavaConversions._
import org.apache.commons.codec.binary.Hex

case class SuperColumn(val name : Array[Byte], 
  val columns : List[Column]) extends Iterable[Column] with Columnar {
  
  def nameAs[T](implicit deserializer : Deserializer[T]) = deserializer.deserialize(name)
  def iterator = columns.iterator
  
  override def toString() : String = {
    "GetSuperColumn(" + Hex.encodeHexString(name) + "," + columns + ")"
  }
}