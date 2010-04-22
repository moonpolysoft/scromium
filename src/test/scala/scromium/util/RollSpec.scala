package scromium.util

import org.specs._
import java.nio.charset.Charset

object StringLit {
  def apply(seq : Char*) : String = {
    new String(List(seq : _*).toArray)
  }
}

class RollSpec extends Specification {
  "Roll" should {
    "do a simple string increment" in {
      val charset = Charset.defaultCharset
      Roll.roll("aaaaaaa") must ==("aaaaaab")
    }
    
    "increment the next character up" in {
      Roll.roll("aaaa" + StringLit(Character.MAX_VALUE)) must ==("aaab" + StringLit(Character.MIN_VALUE))
    }
    
    "increment middle characters" in {
      Roll.roll("aaaa" + StringLit(Character.MAX_VALUE, Character.MAX_VALUE)) must ==("aaab" + StringLit(Character.MIN_VALUE,Character.MIN_VALUE))
    }
    
    "increment at the top" in {
      Roll.roll(StringLit(Character.MAX_VALUE, Character.MAX_VALUE, Character.MAX_VALUE)) must ==(StringLit(Character.MIN_VALUE,Character.MIN_VALUE,Character.MIN_VALUE,Character.MIN_VALUE))
    }
  }
}