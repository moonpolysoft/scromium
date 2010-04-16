package scromium.util

import org.apache.commons.codec.binary.Hex

object HexString {
  def toHexString(ary : Array[Byte]) : String = {
    val builder = new StringBuilder
    for (b <- ary) {
      val hex = (b & 0xFF).toHexString
      if (hex.length < 2) {
        builder ++= "0" + hex
      } else {
        builder ++= hex
      }
    }
    builder.toString
  }
  
  def toBytes(hex : String) : Array[Byte] = {
    Hex.decodeHex(hex.toArray)
  }
}