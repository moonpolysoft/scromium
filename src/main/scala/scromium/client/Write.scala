package scromium.client

import scromium._

case class Write[C](val key : Array[Byte],
  val cf : String,
  val columns : List[C])