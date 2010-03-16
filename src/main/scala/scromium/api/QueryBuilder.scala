package scromium.api

import org.apache.cassandra.thrift
import scala.collection.JavaConversions._
import scala.collection.mutable.HashMap
import scromium._
import serializers._

trait QueryBuilder[S] {
  val predicate = new thrift.SlicePredicate
  val range = new thrift.KeyRange
  
  def keys(startKey : String, endKey : String, limit : Int = 100) : S = {
    range.start_key = startKey
    range.end_key = endKey
    range.count = limit
    this.asInstanceOf[S]
  }
  
  def tokens(startToken : String, endToken : String, limit : Int = 100) : S = {
    range.start_token = startToken
    range.end_token = endToken
    range.count = limit
    this.asInstanceOf[S]
  }
  
  def columns[A](columns : A*)(implicit ser : Serializer[A]) : S = {
    if (predicate.column_names == null) {
      predicate.column_names = new java.util.ArrayList[Array[Byte]]
    }
    predicate.column_names.addAll(columns.map(ser.serialize(_)))
    this.asInstanceOf[S]
  }
  
  def sliceRange[A, B](startColumn : A, endColumn : B, reversed : Boolean = false, limit : Int = 100)(implicit startSer : Serializer[A], endSer : Serializer[B]) : S = {
    if (predicate.slice_range == null) {
      predicate.slice_range = new thrift.SliceRange
    }
    predicate.slice_range.start = startSer.serialize(startColumn)
    predicate.slice_range.finish = endSer.serialize(endColumn)
    predicate.slice_range.count = limit
    this.asInstanceOf[S]
  }
}

class ColumnQueryBuilder(val ks : Keyspace, val cf : String) extends QueryBuilder[ColumnQueryBuilder] {
  val cp = new thrift.ColumnParent
  cp.column_family = cf
  
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

class SuperColumnQueryBuilder(val ks : Keyspace, val cf : String, val superColumn : Array[Byte]) extends QueryBuilder[SuperColumnQueryBuilder] {
  val cp = new thrift.ColumnParent
  cp.column_family = cf
  cp.super_column = superColumn
  
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