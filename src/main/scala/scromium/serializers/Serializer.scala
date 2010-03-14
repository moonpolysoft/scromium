package scromium.serializers

object Serializers {
  implicit object StringSerializer extends Serializer[String] {
    def serialize(str : String) = str.getBytes
    def deserialize(ary : Array[Byte]) = new String(ary)
  }
}

trait Serializer[T] {
  def serialize(obj : T) : Array[Byte]
  def deserialize(ary : Array[Byte]) : T
}


