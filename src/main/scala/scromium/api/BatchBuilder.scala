package scromium.api

import scromium._
import serializers._
import org.apache.cassandra.thrift
import scala.collection.mutable.HashMap
import scala.collection.mutable.ArrayBuffer
import java.util.ArrayList
import scala.collection.JavaConversions._
import scromium.util.HexString._
import scromium.util.Log

class OpMap extends HashMap[String, MuteMap] {
  override def default(key : String) : MuteMap = {
    val m = new MuteMap
    put(key, m)
    m
  }  
}

class MuteMap extends HashMap[String, ArrayBuffer[thrift.Mutation]] {
  override def default(key : String) : ArrayBuffer[thrift.Mutation] ={
    val a = new ArrayBuffer[thrift.Mutation]
    put(key,a)
    a
  } 
}

class BatchBuilder(ks : Keyspace) extends Log {
  type HMuteMap = java.util.HashMap[String, java.util.List[thrift.Mutation]]
  type HOpMap = java.util.HashMap[String, JMuteMap]
  
  type JMuteMap = java.util.Map[String, java.util.List[thrift.Mutation]]
  type JOpMap = java.util.Map[String, JMuteMap]

  val operations = new OpMap
  
  def !(implicit consistency : WriteConsistency) {
    val map = operations.foldLeft(new HOpMap) { (m , tuple) =>
      val(rowKey, cfMap) = tuple
      val mutationMap = cfMap.foldLeft(new HMuteMap) { (m, tuple) =>
        val (cf, mutations) = tuple
        m.put(cf, asList(mutations))
        m
      }
      m.put(rowKey, mutationMap)
      m
    }
    ks.pool.withConnection { conn =>
      debug("batch_mutate(" + ks.name + ", " + map + ", " + consistency.thrift + ")")
      conn.batch_mutate(ks.name, map.asInstanceOf[JOpMap], consistency.thrift)
    }
  }

  def row(row : Array[Byte])(block : RowBatchBuilder => Unit) : this.type = {
    this.row(toHexString(row))(block)
    this
  }  
  def row(row : String)(block : RowBatchBuilder => Unit) : this.type = {
    val builder = new RowBatchBuilder
    val result = block(builder)
    val muteMap = builder.toMap
    muteMap.foreach { case (cf,muteList) => 
      operations(row)(cf).appendAll(muteList)
    }
    this
  }
}

class RowBatchBuilder {
  val muteMap = new MuteMap
  
  def toMap = muteMap
  
  def add[A,B](ins : (String,A), value : B)
    (implicit cSer : Serializer[A],
              vSer : Serializer[B]) : RowBatchBuilder = add(ins, value, System.currentTimeMillis)(cSer,vSer)
    
  def add[A,B](ins : (String,A), value : B, timestamp : Long)
    (implicit cSer : Serializer[A],
              vSer : Serializer[B]) : RowBatchBuilder = {
    val (cf, c) = ins
    val mutation = new thrift.Mutation
    val cAry = cSer.serialize(c.asInstanceOf[A])
    val container = new thrift.ColumnOrSuperColumn
    val column = new thrift.Column
    mutation.column_or_supercolumn = container
    container.column = column
    column.name = cAry
    column.timestamp = timestamp
    column.value = vSer.serialize(value)
    muteMap(cf).append(mutation)
    this
  }
  
  def add[A,B,C](ins : ((String,A),B), value : C)
    (implicit scSer : Serializer[A],
              cSer : Serializer[B],
              vSer : Serializer[C]) : RowBatchBuilder = add(ins, value, System.currentTimeMillis)(scSer, cSer, vSer)
              
  def add[A,B,C](ins : ((String,A),B), value : C, timestamp : Long)
    (implicit scSer : Serializer[A],
              cSer : Serializer[B],
              vSer : Serializer[C]) : RowBatchBuilder = {
    val ((cf, sc), c) = ins
    val ops = muteMap(cf)
    val scAry = scSer.serialize(sc)
    val container = ops.find({ mut => mut.column_or_supercolumn.super_column.name == scAry }).getOrElse {
      val mute = new thrift.Mutation
      val container = new thrift.ColumnOrSuperColumn
      mute.column_or_supercolumn = container
      val superColumn = new thrift.SuperColumn
      container.super_column = superColumn
      ops += mute
      superColumn.name = scAry
      superColumn.columns = new ArrayList[thrift.Column]
      mute
    }
    val column = new thrift.Column
    column.name = cSer.serialize(c)
    column.timestamp = timestamp
    column.value = vSer.serialize(value)
    container.column_or_supercolumn.super_column.columns.add(column)
    this
  }
}
