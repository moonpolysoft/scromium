package scromium.client

import scromium._
import scromium.clocks._

case class Delete(val keys : List[Array[Byte]],
  cf : String,
  columns : Option[List[Array[Byte]]] = None,
  subColumns : Option[List[Array[Byte]]] = None,
  slice : Option[Slice] = None,
  clock : Clock)
  
