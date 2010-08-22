package scromium.thrift

import scromium._

class MultigetRow[T <: Columnar](key : Array[Byte], cols : List[T]) extends Row[T](key) {
  def columns = cols.iterator
}