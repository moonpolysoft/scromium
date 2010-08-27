package scromium

import serializers._

abstract class Row[T <: Columnar](val key : Array[Byte]) {
  def keyAs[K](implicit ser : Deserializer[K]) = ser.deserialize(key)
  
  def columns : Iterator[T]
}