package scromium.client

import scromium._
import scromium.clocks._

case class Delete(val row : Array[Byte],
  cf : String,
  columns : Option[List[Array[Byte]]],
  subcolumns : Option[List[Array[Byte]]],
  slice : Option[Slice],
  clock : Clock)
  
