package scromium.thrift

import scromium._

case class MultigetRow[T <: Columnar](override val key : Array[Byte], cols : List[T]) extends Row[T](key) {
  def columns = cols.iterator
}