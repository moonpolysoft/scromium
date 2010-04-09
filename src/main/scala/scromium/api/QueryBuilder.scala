package scromium.api

import org.apache.cassandra.thrift
import scala.collection.JavaConversions._
import scala.collection.mutable.HashMap
import scromium._
import serializers._
import scromium.util.HexString._
import ContainerFactory._
import scromium.util.Thrift._

abstract class QueryBuilder(ks : Keyspace, cf : String) {
  val cp = new thrift.ColumnParent
  cp.column_family = cf
  
  val predicate = new thrift.SlicePredicate
  
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

abstract class MultiQueryBuilder(ks : Keyspace, cf : String) extends QueryBuilder(ks, cf) {
  val keys = new java.util.ArrayList[String]
  
  def keys(ks : String*) : this.type = {
    ks.foreach { k => keys.add(k) }
    this
  }
  
  protected def execute[A <: Container](consistency : ReadConsistency)(implicit fac : ContainerFactory[A]) : Map[String, Seq[A]] = {
    if (null == predicate.slice_range && null == predicate.column_names) {
      predicate.slice_range = sliceRange("".getBytes, "".getBytes, 100)
    }
    ks.pool.withConnection { conn => 
      val results = conn.client.multiget_slice(ks.name,
        keys,
        cp,
        predicate,
        consistency.thrift)
        
      results.map { case (key, columns) =>
        (key, columns.map{ container => fac.make(container)})
      }.toMap
    }
  }
}

abstract class RangeQueryBuilder(ks : Keyspace, cf : String) extends QueryBuilder(ks, cf) {
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
  
  protected def execute[A <: Container](consistency : ReadConsistency)(implicit fac : ContainerFactory[A]) : Seq[(String, Seq[A])] = {
    if (null == predicate.slice_range && null == predicate.column_names) {
      predicate.slice_range = sliceRange("".getBytes, "".getBytes, 100)
    }
    ks.pool.withConnection { conn =>
      val results = conn.client.get_range_slices(ks.name,
        cp,
        predicate,
        range,
        consistency.thrift)
        
      results.map { ks =>
        (ks.key, ks.columns.map{ container => fac.make(container)})
      }
    }
  }
}

class ColumnRangeQueryBuilder(ks : Keyspace, cf : String) extends RangeQueryBuilder(ks, cf) {
  
  def this(ks : Keyspace, cf : String, superColumn : Array[Byte]) {
    this(ks, cf)
    cp.super_column = superColumn
  }
  
  def !(implicit consistency : ReadConsistency) = execute[GetColumn](consistency)
}

class SuperColumnRangeQueryBuilder(ks : Keyspace, cf : String) extends RangeQueryBuilder(ks, cf) {
  
  def !(implicit consistency : ReadConsistency) = execute[GetSuperColumn](consistency)
}

class ColumnMultiQueryBuilder(ks : Keyspace, cf : String) extends MultiQueryBuilder(ks, cf) {
  
  def this(ks : Keyspace, cf : String, superColumn : Array[Byte]) {
    this(ks, cf)
    cp.super_column = superColumn
  }
  
  def !(implicit consistency : ReadConsistency) = execute[GetColumn](consistency)
}

class SuperColumnMultiQueryBuilder(ks : Keyspace, cf : String) extends MultiQueryBuilder(ks, cf) {
  
  def !(implicit consistency : ReadConsistency) = execute[GetSuperColumn](consistency)
}