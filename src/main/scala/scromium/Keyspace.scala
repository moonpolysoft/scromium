package scromium

import api._
import serializers._
import org.apache.cassandra.thrift
import connection.ConnectionPool
import scromium.util.HexString._
import scromium.util.Log

/*object Keyspace {
  var pool : ConnectionPool = null
  
  def apply[A](ksName : String)(block : Keyspace => A) : A = {
    if (pool == null) {
      throw new Exception("Cassandra client needs to be started first.")
    }
    val ks = new Keyspace(ksName, pool)
    block(ks)
  }
}*/

class Keyspace(val name : String, val pool : ConnectionPool) extends Log {
  
  def apply[A](block : Keyspace => A) : A = {
    block(this)
  }
  
  def get(row : Array[Byte], cf : String) = new CFPath(this, toHexString(row), cf)
  def get(row : String, cf : String) = new CFPath(this, row, cf)
  
  /**
   * Insert using a byte array as the row key and use the default timestmap
   */
  def insert[A, B](row : Array[Byte], ins : (String, A), value : B)
    (implicit cSer : Serializer[A],
              vSer : Serializer[B],
              consistency : WriteConsistency) : Unit = insert(toHexString(row), ins, value)(cSer, vSer, consistency)
  
  def insert[A, B](row : String, ins : (String, A), value : B)
    (implicit cSer : Serializer[A],
              vSer : Serializer[B],
              consistency : WriteConsistency) : Unit = insert(row, ins, value, System.nanoTime)(cSer, vSer, consistency)
  //---------------------------------------------------------------------
  
  //---------------------------------------------------------------------
  //insert with timestamp
  def insert[A, B](row : Array[Byte], ins : (String, A), value : B, timestamp : Long)
    (implicit cSer : Serializer[A],
              vSer : Serializer[B],
              consistency : WriteConsistency) : Unit = insert(toHexString(row), ins, value, timestamp)(cSer, vSer, consistency)
  
  def insert[A, B](row : String, ins : (String, A), value : B, timestamp : Long)
    (implicit cSer : Serializer[A],
              vSer : Serializer[B],
              consistency : WriteConsistency) {
    val (cf, c) = ins
    pool.withConnection { conn =>
      val columnPath = new thrift.ColumnPath
      columnPath.column_family = cf
      columnPath.column = cSer.serialize(c)
      debug { "insert(" + name + ", " + row + ", " + columnPath + ", " + vSer.serialize(value) + ")" }
      conn.insert(name, row, columnPath, vSer.serialize(value), timestamp, consistency.thrift)
    }
  }
  //---------------------------------------------------------------------
  
  //---------------------------------------------------------------------
  //supercolumn insert without timestamp
  def insert[A, B, C](row : Array[Byte], ins : ((String, A), B), value : C)
    (implicit scSer : Serializer[A],
              cSer : Serializer[B],
              vSer : Serializer[C],
              consistency : WriteConsistency) : Unit = insert(toHexString(row), ins, value)(scSer, cSer, vSer, consistency)
  
  def insert[A, B, C](row : String, ins : ((String, A), B), value : C)
    (implicit scSer : Serializer[A],
              cSer : Serializer[B],
              vSer : Serializer[C],
              consistency : WriteConsistency) : Unit = insert(row, ins, value, System.nanoTime)(scSer, cSer, vSer, consistency)
  //---------------------------------------------------------------------
  
  //---------------------------------------------------------------------
  //supercolumn insert with timestamp
  def insert[A, B, C](row : Array[Byte], ins : ((String, A), B), value : C, timestamp : Long)
    (implicit scSer : Serializer[A],
              cSer : Serializer[B],
              vSer : Serializer[C],
              consistency : WriteConsistency) : Unit = insert(toHexString(row), ins, value, timestamp)(scSer, cSer, vSer, consistency)
  
  def insert[A, B, C](row : String, ins : ((String, A), B), value : C, timestamp : Long)
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
      debug {"insert(" + name + ", " + row + ", " + columnPath + ", " + vSer.serialize(value) + ", " + timestamp + ", " + consistency.thrift + ")"}
      conn.insert(name, row, columnPath, vSer.serialize(value), timestamp, consistency.thrift)
    }
  }
  //---------------------------------------------------------------------
  
  def range(cf : String) = new ColumnRangeQueryBuilder(this, cf)
  
  def rangeSuper[A](cf : String, superColumn : A)(implicit ser : Serializer[A]) = new ColumnRangeQueryBuilder(this, cf, ser.serialize(superColumn))
  def rangeSuper(cf : String) = new SuperColumnRangeQueryBuilder(this, cf)
  
  def multiget(cf : String) = new ColumnMultiQueryBuilder(this, cf)
  
  def multigetSuper[A](cf : String, superColumn : A)(implicit ser : Serializer[A]) = new ColumnMultiQueryBuilder(this, cf, ser.serialize(superColumn))
  def multigetSuper(cf : String) = new SuperColumnMultiQueryBuilder(this, cf)
  
  def scan(cf : String) = new ColumnScanBuilder(this, cf)
  
  def scanSuper[A](cf : String, superColumn : A)(implicit ser : Serializer[A]) = new ColumnScanBuilder(this, cf, ser.serialize(superColumn))
  def scanSuper(cf : String) = new SuperColumnScanBuilder(this, cf)
  
  def batch() = new BatchBuilder(this)
}
