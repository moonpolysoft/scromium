package scromium.api

import org.apache.cassandra.thrift
import scala.collection.JavaConversions._
import scala.collection.mutable.HashMap
import scromium._
import serializers._
import scromium.util.HexString._

trait QueryBuilder {
  val predicate = new thrift.SlicePredicate
  val range = new thrift.KeyRange
  
  def keys(startKey : Array[Byte], endKey : Array[Byte]) : this.type = 
    keys(startKey, endKey, 100)
  
  def keys(startKey : Array[Byte], endKey : Array[Byte], limit : Int) : this.type =
    keys(toHexString(startKey), toHexString(endKey), limit)
  
  def keys(startKey : String, endKey : String) : this.type =
    keys(startKey, endKey, 100)
  
  def keys(startKey : String, endKey : String, limit : Int) : this.type = {
    range.start_key = startKey
    range.end_key = endKey
    range.count = limit
    this
  }
  
  def columns[A](columns : A*)(implicit ser : Serializer[A]) : this.type = {
    if (predicate.column_names == null) {
      predicate.column_names = new java.util.ArrayList[Array[Byte]]
    }
    predicate.column_names.addAll(columns.map(ser.serialize(_)))
    this
  }
  
  def columnRange[A, B](startColumn : A, endColumn : B, reversed : Boolean = false, limit : Int = 100)(implicit startSer : Serializer[A], endSer : Serializer[B]) : this.type = {
    if (predicate.slice_range == null) {
      predicate.slice_range = new thrift.SliceRange
    }
    predicate.slice_range.start = startSer.serialize(startColumn)
    predicate.slice_range.finish = endSer.serialize(endColumn)
    predicate.slice_range.count = limit
    this
  }
}

class ColumnQueryBuilder(val ks : Keyspace, val cf : String) extends QueryBuilder {
  val cp = new thrift.ColumnParent
  cp.column_family = cf
  
  def this(ks : Keyspace, cf : String, superColumn : Array[Byte]) {
    this(ks, cf)
    cp.super_column = superColumn
  }
  
  def !(implicit consistency : ReadConsistency) : Seq[(String, Seq[GetColumn])] = {
    ks.pool.withConnection { conn =>
      val results = conn.client.get_range_slices(ks.name,
        cp,
        predicate,
        range,
        consistency.thrift)
        
      results.map { ks =>
        (ks.key, ks.columns.map{ container => new GetColumn(container.column)})
      }
    }
  }
}

class SuperColumnQueryBuilder(val ks : Keyspace, val cf : String) extends QueryBuilder {
  val cp = new thrift.ColumnParent
  cp.column_family = cf
  
  def !(implicit consistency : ReadConsistency) : Seq[(String, Seq[GetSuperColumn])] = {
    ks.pool.withConnection { conn => 
      val results = conn.client.get_range_slices(ks.name,
        cp,
        predicate,
        range,
        consistency.thrift)
      
      results.map { ks =>
        (ks.key, ks.columns.map{ container => new GetSuperColumn(container.super_column)})
      }
    }
  }
}