package scromium

case class Row[T](val key : Array[Byte], val columns : List[T])