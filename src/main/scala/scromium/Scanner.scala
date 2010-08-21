package scromium

import serializers._

object Scanner {
  def apply[R](start : R, end : R)(implicit ser : Serializer[R]) =
    new Scanner(ser.serialize(start), ser.serialize(end))
}

class Scanner(val startKey : Array[Byte], val endKey : Array[Byte]) {
  var limit = 1000
  var fetchLimit = 100
  
}