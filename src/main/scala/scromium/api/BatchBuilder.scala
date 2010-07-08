package scromium.api

import scromium._
import serializers._
import org.apache.cassandra.thrift
import java.util.{HashMap,ArrayList,Map,List}
import scala.collection.JavaConversions._
import scromium.util.HexString._
import scromium.util.Log

class OpMap extends HashMap[String, MuteMap](100) {
  def get(key : String) : MuteMap = {
    super.get(key) match {
      case null =>
        val m = new MuteMap
        put(key, m)
        m
      case v => v
    }
  }
}

class MuteMap extends HashMap[String, List[thrift.Mutation]](100) {
  def get(key : String) : List[thrift.Mutation] ={
    super.get(key) match {
      case null =>
        val a = new ArrayList[thrift.Mutation](20)
        put(key,a)
        a
      case v => v
    }
  }
}

class BatchBuilder(ks : Keyspace) extends Log {

  val operations = new OpMap
  
  def !(implicit consistency : WriteConsistency) {
    ks.pool.withConnection { conn =>
      debug("batch_mutate(" + ks.name + ", " + operations + ", " + consistency.thrift + ")")
      conn.batch_mutate(ks.name, operations.asInstanceOf[Map[String,Map[String,List[thrift.Mutation]]]], consistency.thrift)
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
    muteMap.entrySet.foreach { entry => 
      operations.get(row).get(entry.getKey).addAll(entry.getValue)
    }
    this
  }
}

class RowBatchBuilder {
  val muteMap = new MuteMap
  
  def toMap = muteMap
  
  def add[A,B](ins : (String,A), value : B)
    (implicit cSer : Serializer[A],
              vSer : Serializer[B]) : RowBatchBuilder = add(ins, value, Clock.timestamp)(cSer,vSer)
    
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
    muteMap.get(cf).add(mutation)
    this
  }
  
  def add[A,B,C](ins : ((String,A),B), value : C)
    (implicit scSer : Serializer[A],
              cSer : Serializer[B],
              vSer : Serializer[C]) : RowBatchBuilder = add(ins, value, Clock.timestamp)(scSer, cSer, vSer)
              
  def add[A,B,C](ins : ((String,A),B), value : C, timestamp : Long)
    (implicit scSer : Serializer[A],
              cSer : Serializer[B],
              vSer : Serializer[C]) : RowBatchBuilder = {
    val ((cf, sc), c) = ins
    val ops = muteMap.get(cf)
    val scAry = scSer.serialize(sc)
    val container = ops.find({ mut => mut.column_or_supercolumn.super_column.name == scAry }).getOrElse {
      val mute = new thrift.Mutation
      val container = new thrift.ColumnOrSuperColumn
      mute.column_or_supercolumn = container
      val superColumn = new thrift.SuperColumn
      container.super_column = superColumn
      ops add mute
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
