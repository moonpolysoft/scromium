package scromium.util

import scala.collection.mutable.{HashMap, WrappedArray}

class ArrayKeyedHashMap[A,B](d : Array[A] => B) extends DefaultHashMap[Array[A],B](d) {
  
  override protected def elemEquals(key1: Array[A], key2: Array[A]): Boolean = {
    (WrappedArray.make(key1) == WrappedArray.make(key2))
  }

  override protected def elemHashCode(key: Array[A]) : Int = {
    if (key == null) 0 else WrappedArray.make(key).hashCode()
  }
  
}