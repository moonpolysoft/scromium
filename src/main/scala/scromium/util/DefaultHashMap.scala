package scromium.util

import scala.collection.mutable.HashMap

class DefaultHashMap[A,B](d : A => B) extends HashMap[A,B] {
  
  override def default(key : A) : B = {
    val result = d(key)
    put(key, result)
    result
  }
  
}