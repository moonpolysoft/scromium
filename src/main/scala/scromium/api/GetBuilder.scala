package scromium.api

import org.apache.cassandra.thrift
import scromium._
import serializers._
import connection._


case class CFPath(keyspace : Keyspace, row : String, cf : String) {
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
      val result = connection.client.get(cfpath.keyspace.name, 
        cfpath.row,
        thriftCP,
        consistency.thrift)
      new GetColumn(result)
    }
  }
}

case class SuperColumnPath(cfpath : CFPath, superColumn : Array[Byte]) {
  def !(implicit consistency : ReadConsistency) : GetSuperColumn = {
    cfpath.keyspace.pool.withConnection { connection => 
      val thriftCP = new thrift.ColumnPath
      thriftCP.column_family = cfpath.cf
      thriftCP.super_column = superColumn
      val result = connection.client.get(cfpath.keyspace.name,
        cfpath.row,
        thriftCP,
        consistency.thrift)
      new GetSuperColumn(result)
    }
  }
}