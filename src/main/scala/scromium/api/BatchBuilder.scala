package scromium.api

import scromium._
import serializers._
import org.apache.cassandra.thrift
import scala.collection.mutable.HashMap
import scala.collection.mutable.ArrayBuffer
import java.util.ArrayList
import scala.collection.JavaConversions._

class BatchBuilder(ks : Keyspace, row : String) {
  val operations = new HashMap[String, ArrayBuffer[thrift.ColumnOrSuperColumn]]
  
  def !(implicit consistency : WriteConsistency) {
    ks.pool.withConnection { conn =>
      val map = operations.foldLeft(new java.util.HashMap[String, java.util.List[thrift.ColumnOrSuperColumn]]) { (m , tuple) =>
        val (cf, list) = tuple
        m.put(cf, asList(list))
        m
      }
      conn.client.batch_insert(ks.name, row, map, consistency.thrift)
    }
  }
  
  def add[A,B](ins : Tuple2[String,A], value : B)
    (implicit cSer : Serializer[A],
              vSer : Serializer[B]) : BatchBuilder = add(ins, value, System.currentTimeMillis)(cSer, vSer)
  
  def add[A,B](ins : Tuple2[String,A], value : B, timestamp : Long)
    (implicit cSer : Serializer[A],
              vSer : Serializer[B]) : BatchBuilder = ins match {
                case (cf : String, c : A) =>
                  val ops = operations.getOrElseUpdate(cf, new ArrayBuffer[thrift.ColumnOrSuperColumn])
                  val cAry = cSer.serialize(c)
                  val container = new thrift.ColumnOrSuperColumn
                  val column = new thrift.Column
                  container.column = column
                  ops += container
                  column.name = cAry
                  column.timestamp = timestamp
                  column.value = vSer.serialize(value)
                  this
              }
  
  def add[A,B,C](ins : Tuple2[Tuple2[String, A],B], value : C)
    (implicit scSer : Serializer[A],
              cSer : Serializer[B],
              vSer : Serializer[C]) : BatchBuilder = add(ins, value, System.currentTimeMillis)(scSer,cSer,vSer)
  
  def add[A,B,C](ins : Tuple2[Tuple2[String, A],B], value : C, timestamp : Long)
    (implicit scSer : Serializer[A],
              cSer : Serializer[B],
              vSer : Serializer[C]) : BatchBuilder = ins match {
    case ((cf : String, sc : A), c : B) =>
      val ops = operations.getOrElseUpdate(cf, new ArrayBuffer[thrift.ColumnOrSuperColumn])
      val scAry = scSer.serialize(sc)
      val container = ops.find({ container => container.super_column.name == scAry }).getOrElse {
        val container = new thrift.ColumnOrSuperColumn
        val superColumn = new thrift.SuperColumn
        container.super_column = superColumn
        ops += container
        superColumn.name = scAry
        superColumn.columns = new ArrayList[thrift.Column]
        container
      }
      val column = new thrift.Column
      column.name = cSer.serialize(c)
      column.timestamp = timestamp
      column.value = vSer.serialize(value)
      container.super_column.columns.add(column)
      this
  }
}