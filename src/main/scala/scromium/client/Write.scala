package scromium.client

import scromium._

class Write[C](val key : Array[Byte],
  val cf : String,
  val columns : List[C])