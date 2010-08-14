package scromium.serializers

trait IntegralSerializer {
  
  def longToBytes(value : Long) : Array[Byte] = {
    if (value >= Byte.MinValue && value <= Byte.MaxValue) {
      __byteConversion(value.toByte)
    } else if (value >= Short.MinValue && value <= Short.MaxValue) {
      __byteConversion(value.toShort)
    } else if (value >= Int.MinValue && value <= Int.MaxValue) {
      __byteConversion(value.toInt)
    } else {
      __byteConversion(value)
    }
  }

  def __byteConversion(v : Long) : Array[Byte] = {
    val ba = new Array[Byte](8)
    ba(0) = (0xff & (v >> 56)).toByte
    ba(1) = (0xff & (v >> 48)).toByte
    ba(2) = (0xff & (v >> 40)).toByte
    ba(3) = (0xff & (v >> 32)).toByte
    ba(4) = (0xff & (v >> 24)).toByte
    ba(5) = (0xff & (v >> 16)).toByte
    ba(6) = (0xff & (v >>  8)).toByte
    ba(7) = (0xff & v).toByte
    ba
  }

  def __byteConversion(v : Int) : Array[Byte] = {
    val ba = new Array[Byte](4)
    ba(0) = (0xff & (v >> 24)).toByte
    ba(1) = (0xff & (v >> 16)).toByte
    ba(2) = (0xff & (v >> 8)).toByte
    ba(3) = (0xff & v).toByte
    ba
  }

  def __byteConversion(v : Short) : Array[Byte] = {
    val ba = new Array[Byte](2)
    ba(0) = (0xff & (v >> 8)).toByte
    ba(1) = (0xff & v).toByte
    ba
  }

  def __byteConversion(v : Byte) : Array[Byte]= Array(v)
}

trait IntegralDeserializer {
  def bytesToLong(data : Seq[Byte]) : Long = {
    if (data.length >= 8) {
      (((data(0) & 0xff).toLong << 56) |
        ((data(1) & 0xff).toLong << 48) |
        ((data(2) & 0xff).toLong << 40) |
        ((data(3) & 0xff).toLong << 32) |
        ((data(4) & 0xff).toLong << 24) |
        ((data(5) & 0xff).toLong << 16) |
        ((data(6) & 0xff).toLong <<  8) |
        ((data(7) & 0xff).toLong))
    } else if (data.length >= 4) {
      (((data(0) & 0xff) << 24) | ((data(1) & 0xff) << 16) |
        ((data(2) & 0xff) << 8) | (data(3) & 0xff))
    } else if (data.length >= 2) {
      ((data(0).toShort << 8) | (data(1).toShort & 0xff))
    } else {
      data(0)
    }
  }
  
  def bytesToInt(data : Seq[Byte]) : Int = bytesToLong(data).toInt
  def bytesToShort(data : Seq[Byte]) : Short = bytesToLong(data).toShort
  def bytesToByte(data : Seq[Byte]) : Byte = bytesToLong(data).toByte
  def bytesToBoolean(data : Seq[Byte]) : Boolean = data(0) match {
    case 1 => true
    case _ => false
  }
}