package scromium.api

import org.specs._
import org.specs.mock.Mockito
import org.mockito.Matchers._
import org.apache.cassandra.thrift
import scromium._
import connection._
import serializers._
import Serializers._
import java.util.HashMap
import java.util.ArrayList

class BatchBuilderSpec extends Specification with Mockito with TestHelper {
  "BatchBuilder" should {
    "execute a batch_insert with a super column" in {
      val client = clientSetup
      val timestamp = System.currentTimeMillis
      val map = new HashMap[String, java.util.List[thrift.ColumnOrSuperColumn]]
      val list = new ArrayList[thrift.ColumnOrSuperColumn]
      val column = new thrift.Column
      column.name = "c".getBytes
      column.value = "value".getBytes
      column.timestamp = timestamp
      val superColumn = new thrift.SuperColumn
      superColumn.name = "sc".getBytes
      superColumn.columns = new ArrayList[thrift.Column]
      superColumn.columns.add(column)
      val container = new thrift.ColumnOrSuperColumn
      container.super_column = superColumn
      val operations = new ArrayList[thrift.ColumnOrSuperColumn]
      operations.add(container)
      map.put("cf", operations)
      
      Keyspace("ks") {ks =>
        implicit val consistency = WriteConsistency.Any
        val batch = ks.batch("row")
        batch.add("cf" -> "sc" -> "c", "value", timestamp)
        batch!
      }
      
      client.batch_insert("ks", "row", map, WriteConsistency.Any.thrift) was called
    }
    
    "execute a batch_insert with a single column" in {
      val client = clientSetup
      val timestamp = System.currentTimeMillis
      val map = new HashMap[String, java.util.List[thrift.ColumnOrSuperColumn]]
      val list = new ArrayList[thrift.ColumnOrSuperColumn]
      val column = new thrift.Column
      column.name = "c".getBytes
      column.value = "value".getBytes
      column.timestamp = timestamp
      val container = new thrift.ColumnOrSuperColumn
      container.column = column
      val operations = new ArrayList[thrift.ColumnOrSuperColumn]
      operations.add(container)
      map.put("cf", operations)
      
      Keyspace("ks") {ks =>
        implicit val consistency = WriteConsistency.Any
        val batch = ks.batch("row")
        batch.add("cf" -> "c", "value", timestamp)
        batch!
      }
      
      client.batch_insert("ks", "row", map, WriteConsistency.Any.thrift) was called
    }
    
    "pick the right serializer" in {
      val client = clientSetup
      val timestamp = System.currentTimeMillis
      val map = new HashMap[String, java.util.List[thrift.ColumnOrSuperColumn]]
      val list = new ArrayList[thrift.ColumnOrSuperColumn]
      val column = new thrift.Column
      column.name = "c".getBytes
      column.value = Array[Byte](0)
      column.timestamp = timestamp
      val container = new thrift.ColumnOrSuperColumn
      container.column = column
      val operations = new ArrayList[thrift.ColumnOrSuperColumn]
      operations.add(container)
      map.put("cf", operations)
      
      class Fuck {}
      
      implicit object FuckSerializer extends Serializer[Fuck] {
        def serialize(fuck : Fuck) = Array[Byte](0)
        
        def deserialize(ary : Array[Byte]) = new Fuck
      }
      
      Keyspace("ks") {ks =>
        implicit val consistency = WriteConsistency.Any
        val batch = ks.batch("row")
        batch.add("cf" -> "c", new Fuck, timestamp)
        batch!
      }
      
      client.batch_insert("ks", "row", map, WriteConsistency.Any.thrift) was called
    }
    
    "pick a covariant serializer" in {
      val client = clientSetup
      val timestamp = System.currentTimeMillis
      val map = new HashMap[String, java.util.List[thrift.ColumnOrSuperColumn]]
      val list = new ArrayList[thrift.ColumnOrSuperColumn]
      val column = new thrift.Column
      column.name = "c".getBytes
      column.value = Array[Byte](0)
      column.timestamp = timestamp
      val container = new thrift.ColumnOrSuperColumn
      container.column = column
      val operations = new ArrayList[thrift.ColumnOrSuperColumn]
      operations.add(container)
      map.put("cf", operations)
      
      trait Balls
      
      class Fuck extends Balls
      
      implicit object BallsSerializer extends Serializer[Balls] {
        def serialize(fuck : Balls) = Array[Byte](0)
        
        def deserialize(ary : Array[Byte]) : Balls = new Fuck
      }
      
      Keyspace("ks") {ks =>
        implicit val consistency = WriteConsistency.Any
        val batch = ks.batch("row")
        batch.add("cf" -> "c", new Fuck, timestamp)
        batch!
      }
      
      client.batch_insert("ks", "row", map, WriteConsistency.Any.thrift) was called
    }
  }
}
