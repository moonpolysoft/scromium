package scromium.util

import scala.annotation.tailrec

object Roll {
  def roll(str : String) : String = {
    @tailrec
    def reverseRoll(str : List[Char], acc: List[Char]) : List[Char] = str match {
      case Nil => Character.MIN_VALUE :: acc
      case Character.MAX_VALUE :: tail =>
        reverseRoll(str.tail, Character.MIN_VALUE :: acc)
      case head :: tail =>
        tail.reverse ++ List((head.toInt + 1).toChar) ++ acc
    }
    val chars = reverseRoll(str.reverse.toCharArray.toList, Nil)
    new String(chars.toArray)
  }
}