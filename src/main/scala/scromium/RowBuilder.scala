package scromium

import serializers._
import clocks._
import client._
import scala.collection.mutable.ArrayBuffer

class RowBuilder(val key : Array[Byte], clock : Clock) {
  val columns = new ArrayBuffer[Column]
  
  def insert[C,V](column : C, value : V, timestamp : Long = clock.timestamp, ttl : Option[Int] = None)
    (implicit cSer : Serializer[C],
              vSer : Serializer[V]) : this.type = {
    columns += Column(cSer.serialize(column), vSer.serialize(value), timestamp, ttl)
    this
  }
  
  def toWrite(cf : String) = new Write(key, cf, columns.toList)
}

class SuperRowBuilder(val key : Array[Byte], clock : Clock) {
  val superColumns = new ArrayBuffer[SuperColumnBuilder]
  
  def superColumn[C](superColumn : C)(implicit ser : Serializer[C]) : SuperColumnBuilder = {
    val sc = new SuperColumnBuilder(ser.serialize(superColumn), clock)
    superColumns += sc
    sc
  }
  
  def toWrite(cf : String) = new Write(key, cf, superColumns.map(_.toSuperColumn).toList)
}