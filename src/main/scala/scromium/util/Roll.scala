package scromium.util

object Roll {
  def roll(str : String) : String = {
    def reverseRoll(str : List[Byte], acc: List[Byte]) : List[Byte] = str match {
      case Nil => 0.toByte :: acc
      case -1 :: tail =>
        println("recursing")
        reverseRoll(str.tail, 0.toByte :: acc)
      case head :: tail =>
        println("fuck " + head)
        tail.reverse ++ List(((head + 1).toByte)) ++ acc
    }
    val chars = reverseRoll(str.reverse.getBytes.toList, Nil)
    new String(chars.toArray)
  }
}