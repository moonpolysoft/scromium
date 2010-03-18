package scromium.serializers

object Serializers {
  implicit object StringSerializer extends Serializer[String] with Deserializer[String] {
    def serialize(str : String) = str.asInstanceOf[String].getBytes
    def deserialize(ary : Array[Byte]) = new String(ary)
  }
}

trait Serializer[-T] {
  def serialize(obj : T) : Array[Byte]
}

trait Deserializer[+T] {
  def deserialize(ary : Array[Byte]) : T
}