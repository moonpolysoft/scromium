package scromium

import api._
import serializers._
import org.apache.cassandra.thrift
import connection.ConnectionPool

object Keyspace {
  var pool : ConnectionPool = null
  
  def apply(ksName : String)(block : Keyspace => Any) : Any = {
    if (pool == null) {
      throw new Exception("Cassandra client needs to be started first.")
    }
    val ks = new Keyspace(ksName, pool)
    block(ks)
  }
}

class Keyspace(val name : String, val pool : ConnectionPool) {
  def get(row : String, cf : String) = new CFPath(this, row, cf)
  
  def insert[A, B, C](row : String, ins : Tuple2[_, _], value : C, timestamp : Long = System.currentTimeMillis)
    (implicit scSer : Serializer[A],
              cSer : Serializer[B],
              vSer : Serializer[C],
              consistency : WriteConsistency) {
    ins match {
      case ((cf : String, sc : A), c : B) =>
        pool.withConnection { conn =>
          val columnPath = new thrift.ColumnPath
          columnPath.column_family = cf
          columnPath.super_column = scSer.serialize(sc)
          columnPath.column = cSer.serialize(c)
          conn.client.insert(name, row, columnPath, vSer.serialize(value), timestamp, consistency.thrift)
        }
      case (cf : String , c : B) =>
        pool.withConnection { conn =>
          val columnPath = new thrift.ColumnPath
          columnPath.column_family = cf
          columnPath.column = cSer.serialize(c)
          conn.client.insert(name, row, columnPath, vSer.serialize(value), timestamp, consistency.thrift)
        }
    }
  }
  
  def rangeSlices(cf : String) = new ColumnQueryBuilder(this, cf)
  
  def rangeSlices[A](cf : String, superColumn : A)(implicit ser : Serializer[A]) = new SuperColumnQueryBuilder(this, cf, ser.serialize(superColumn))
  
  def batch(row : String) = new BatchBuilder(this, row)
}
