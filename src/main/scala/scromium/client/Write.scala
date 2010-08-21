package scromium.client

import scromium._

class Write[C](val row : Array[Byte],
  val cf : String,
  val columns : List[C])