package scromium.util

import org.specs._
import HexString._

class HexStringSpec extends Specification {
  "HexString" should {
    "serialize stuff" in {
      toHexString(Array(0xFF.toByte)) must ==("ff")
      toHexString(Array(0xf.toByte, 0xaa.toByte)) must ==("0faa")
    }
  }
}