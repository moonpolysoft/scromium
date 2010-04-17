package scromium.util

import org.specs._

object StringLit {
  def apply(seq : Int*) : String = {
    new String(List(seq.map(_.toByte) : _*).toArray)
  }
}

class RollSpec extends Specification {
  "Roll" should {
    "do a simple string increment" in {
      Roll.roll("aaaaaaa") must ==("aaaaaab")
    }
    
    "increment the next character up" in {
      Roll.roll("aaaa" + StringLit(255)) must ==("aaab" + StringLit(0))
    }
    
    "increment middle characters" in {
      Roll.roll("aaaa" + StringLit(255, 255)) must ==("aaab" + StringLit(0,0))
    }
    
    "increment at the top" in {
      Roll.roll(StringLit(255, 255, 255)) must ==(StringLit(0,0,0,0))
    }
  }
}