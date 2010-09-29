package scromium

import serializers._
import util._
import scala.collection.JavaConversions._

case class SuperColumn(val name : Array[Byte], 
  val columns : List[Column]) extends Iterable[Column] with Columnar {
  
  def nameAs[T](implicit deserializer : Deserializer[T]) = deserializer.deserialize(name)
  def iterator = columns.iterator
  
  override def toString() : String = {
    "SuperColumn(" + HexString.toHexString(name) + "," + columns + ")"
  }
}