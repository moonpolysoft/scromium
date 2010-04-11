package scromium.api

import org.apache.cassandra.thrift
import scromium._
import serializers._
import connection._
import scromium.util.HexString._


case class CFPath(keyspace : Keyspace, row : String, cf : String) {
  def this(keyspace : Keyspace, row : Array[Byte], cf : String) {
    this(keyspace, toHexString(row), cf)
  }
  
  def /[T](column : T)(implicit serializer : Serializer[T]) = 
    new ColumnPath(this, serializer.serialize(column))
    
  def %[T](superColumn : T)(implicit serializer : Serializer[T]) =
    new SuperColumnPath(this, serializer.serialize(superColumn))
}

case class ColumnPath(cfpath : CFPath, column : Array[Byte]) {
  def !(implicit consistency : ReadConsistency) : GetColumn = {
    cfpath.keyspace.pool.withConnection { connection =>
      val thriftCP = new thrift.ColumnPath
      thriftCP.column_family = cfpath.cf
      thriftCP.column = column
      val result = connection.get(cfpath.keyspace.name, 
        cfpath.row,
        thriftCP,
        consistency.thrift)
      new GetColumn(result)
    }
  }
}

case class SuperColumnPath(val cfpath : CFPath, val superColumn : Array[Byte]) {
  def /[T](column : T)(implicit serializer : Serializer[T]) =
    new FullPath(this, serializer.serialize(column))
  
  def !(implicit consistency : ReadConsistency) : GetSuperColumn = {
    cfpath.keyspace.pool.withConnection { connection => 
      val thriftCP = new thrift.ColumnPath
      thriftCP.column_family = cfpath.cf
      thriftCP.super_column = superColumn
      val result = connection.get(cfpath.keyspace.name,
        cfpath.row,
        thriftCP,
        consistency.thrift)
      new GetSuperColumn(result)
    }
  }
}

case class FullPath(path : SuperColumnPath, column : Array[Byte]) {
  def !(implicit consistency : ReadConsistency) : GetColumn = {
    path.cfpath.keyspace.pool.withConnection { connection =>
      val thriftCP = new thrift.ColumnPath
      thriftCP.column_family = path.cfpath.cf
      thriftCP.super_column = path.superColumn
      thriftCP.column = column
      val result = connection.get(path.cfpath.keyspace.name,
        path.cfpath.row,
        thriftCP,
        consistency.thrift)
      new GetColumn(result)
    }
  }
}