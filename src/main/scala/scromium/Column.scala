package scromium

import scromium.serializers.Deserializer
import org.apache.commons.codec.binary.Hex

case class Column(val name : Array[Byte], 
                  val value : Array[Byte], 
                  val timestamp : Long, 
                  val ttl : Option[Int]) extends Columnar {
  def valueAs[T](implicit des : Deserializer[T]) = des.deserialize(value)
  def nameAs[T](implicit des : Deserializer[T]) = des.deserialize(name)
  
  override def toString() : String = {
    "Column(" + Hex.encodeHexString(name) + "," + Hex.encodeHexString(value) + "," + timestamp + ")"
  }
}