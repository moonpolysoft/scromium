package scromium

object ByteArray {
  def apply(xs : Int*) : Array[Byte] = {
    xs.map({x => x.toByte}).toArray
  }
}