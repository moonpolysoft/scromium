package scromium

import org.specs._

class ClockSpec extends Specification {
  "Clock" should {
    "return the current time in microseconds" in {
      (Clock.timestamp / 1000) must beCloseTo(System.currentTimeMillis, 100)
    }
  }
}