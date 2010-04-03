package scromium.util

import org.specs._

class JSONSpec extends Specification {
  "JSON" should {
    "parse a normal object" in {
      val map = JSON.parseObject(" {\"host\":\"data1\",\"port\":4196,\"maxIdle\":10,\"initCapacity\":3}")
      println("map " + map.getClass)
      map("host") must ==("data1")
    }
  }
}