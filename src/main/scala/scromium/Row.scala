package scromium

import serializers._

abstract class Row[T <: Columnar](val key : Array[Byte]) {
  def columns : Iterator[T]
}