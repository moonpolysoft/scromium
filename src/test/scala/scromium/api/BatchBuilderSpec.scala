package scromium.api

import org.specs._
import org.specs.mock.Mockito
import org.mockito.Matchers._
import org.apache.cassandra.thrift
import scromium._
import connection._
import serializers.Serializers._
import java.util.HashMap
import java.util.ArrayList

class BatchBuilderSpec extends Specification with Mockito with TestHelper {
  "BatchBuilder" should {
    "execute a batch_insert with a super column" in {
      val connection = mock[Connection]
      val pool = fakeConnectionPool(connection)
      val client = mock[thrift.Cassandra.Client]
      Keyspace.pool = pool
      connection.client returns client
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
  }
}
