package scromium.serializers

object Serializers {
  implicit object StringSerializer extends Serializer[String] with Deserializer[String] {
    def serialize(str : String) = str.asInstanceOf[String].getBytes
    def deserialize(ary : Array[Byte]) = Some(new String(ary))
  }
  
  implicit object ByteArraySerializer extends Serializer[Array[Byte]] with Deserializer[Array[Byte]] {
    def serialize(ary : Array[Byte]) = ary
    def deserialize(ary : Array[Byte]) = Some(ary)
  }
  
  implicit object ByteSeqSerializer extends Serializer[Seq[Byte]] with Deserializer[Seq[Byte]] {
    def serialize(seq : Seq[Byte]) = seq.toArray
    def deserialize(ary : Array[Byte]) = Some(ary.toSeq)
  }
  
  implicit object LongSerializer extends Serializer[Long] with Deserializer[Long] with 
      IntegralSerializer with IntegralDeserializer {
    def serialize(n : Long) = longToBytes(n)
    def deserialize(ary : Array[Byte]) = Some(bytesToLong(ary))
  }
  
  implicit object IntSerializer extends Serializer[Int] with Deserializer[Int] with
      IntegralSerializer with IntegralDeserializer {
    def serialize(n : Int) = longToBytes(n)
    def deserialize(ary : Array[Byte]) = Some(bytesToLong(ary).toInt)
  }
}

trait Serializer[-T] {
  def serialize(obj : T) : Array[Byte]
}

trait Deserializer[+T] {
  def deserialize(ary : Array[Byte]) : Option[T]
}