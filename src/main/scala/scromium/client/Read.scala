package scromium.client

import scromium._

case class Read(val row : Array[Byte],
  val columnFamily : String,
  val columns : Option[List[Array[Byte]]] = None,
  val subColumns : Option[List[Array[Byte]]] = None,
  val slice : Option[Slice] = None)
