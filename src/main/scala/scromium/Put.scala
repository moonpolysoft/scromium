package scromium

import scala.collection.mutable.ArrayBuffer
import serializers._
import scromium.clocks._

class Put(clock : Clock) {
  val rows = new ArrayBuffer[RowBuilder]
  
  def row[R](key : R)(implicit ser : Serializer[R]) : RowBuilder = {
    val row = new RowBuilder(ser.serialize(key), clock)
    rows += row
    row
  }
}

class SuperPut(clock : Clock) {
  val rows = new ArrayBuffer[SuperRowBuilder]
  
  def row[R](key : R)(implicit ser : Serializer[R]) : SuperRowBuilder = {
    val row = new SuperRowBuilder(ser.serialize(key), clock)
    rows += row
    row
  }
}