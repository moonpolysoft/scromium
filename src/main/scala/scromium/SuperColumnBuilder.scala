package scromium

import serializers._
import clocks._
import scala.collection.mutable.ArrayBuffer

class SuperColumnBuilder(val name : Array[Byte], clock : Clock) {
  val columns = new ArrayBuffer[Column]
  
  def insert[C,V](column : C, value : V, timestamp : Long = clock.timestamp, ttl : Option[Int] = None)
    (implicit cSer : Serializer[C],
              vSer : Serializer[V]) : this.type = {
    columns += Column(cSer.serialize(column), vSer.serialize(value), timestamp, ttl)
    this
  }
}