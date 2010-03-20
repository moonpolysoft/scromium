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
  
  def insert[A, B](row : String, ins : Tuple2[String, A], value : B)
    (implicit cSer : Serializer[A],
              vSer : Serializer[B],
              consistency : WriteConsistency) : Unit = insert(row, ins, value, System.currentTimeMillis)(cSer, vSer, consistency)
  
  def insert[A, B](row : String, ins : Tuple2[String, A], value : B, timestamp : Long)
    (implicit cSer : Serializer[A],
              vSer : Serializer[B],
              consistency : WriteConsistency) {
    val (cf, c) = ins
    pool.withConnection { conn =>
      val columnPath = new thrift.ColumnPath
      columnPath.column_family = cf
      columnPath.column = cSer.serialize(c)
      conn.client.insert(name, row, columnPath, vSer.serialize(value), timestamp, consistency.thrift)
    }
  }
  
  def insert[A, B, C](row : String, ins : Tuple2[Tuple2[String, A], B], value : C)
    (implicit scSer : Serializer[A],
              cSer : Serializer[B],
              vSer : Serializer[C],
              consistency : WriteConsistency) : Unit = insert(row, ins, value, System.currentTimeMillis)(scSer, cSer, vSer, consistency)
  
  def insert[A, B, C](row : String, ins : Tuple2[Tuple2[String, A], B], value : C, timestamp : Long)
    (implicit scSer : Serializer[A],
              cSer : Serializer[B],
              vSer : Serializer[C],
              consistency : WriteConsistency) {
    val ((cf, sc), c) = ins
    pool.withConnection { conn =>
      val columnPath = new thrift.ColumnPath
      columnPath.column_family = cf
      columnPath.super_column = scSer.serialize(sc)
      columnPath.column = cSer.serialize(c)
      conn.client.insert(name, row, columnPath, vSer.serialize(value), timestamp, consistency.thrift)
    }
  }
  
  def query(cf : String) = new ColumnQueryBuilder(this, cf)
  
  def querySuper[A](cf : String, superColumn : A)(implicit ser : Serializer[A]) = new ColumnQueryBuilder(this, cf, ser.serialize(superColumn))
  def querySuper(cf : String) = new SuperColumnQueryBuilder(this, cf)
  
  def scan(cf : String) = new ColumnScanBuilder(this, cf)
  
  def scanSuper[A](cf : String, superColumn : A)(implicit ser : Serializer[A]) = new ColumnScanBuilder(this, cf, ser.serialize(superColumn))
  def scanSuper(cf : String) = new SuperColumnScanBuilder(this, cf)
  
  def batch(row : String) = new BatchBuilder(this, row)
}
