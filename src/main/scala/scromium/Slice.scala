package scromium

import serializers._

object Slice {
  def apply[C](start : C, end : C, reversed : Boolean = false, limit : Option[Int] = None)(implicit cSer : Serializer[C]) = 
    new Slice(cSer.serialize(start), cSer.serialize(end), reversed, limit)
}

class Slice(val start : Array[Byte], val end : Array[Byte], val reversed : Boolean, val limit : Option[Int])
