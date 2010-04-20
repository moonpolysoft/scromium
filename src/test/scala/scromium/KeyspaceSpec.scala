package scromium

import org.specs._
import org.specs.mock.Mockito
import org.apache.cassandra.thrift
import org.mockito.Matchers._
import scromium._
import connection._
import api._
import serializers.Serializers._

class KeyspaceSpec extends Specification with Mockito with TestHelper {
  "Keyspace" should {
    "should perform a single column insert" in {
      val (cassandra,client) = clientSetup
      val cp = new thrift.ColumnPath
      cp.column_family = "cf"
      cp.column = "c".getBytes
      val cons = thrift.ConsistencyLevel.ANY
      val timestamp = Clock.timestamp
      
      cassandra.keyspace("ks") { ks =>
        implicit val consistency = WriteConsistency.Any
        ks.insert("row", "cf" -> "c", "value", timestamp)
      }
      
      client.insert("ks", "row", cp, "value".getBytes, timestamp, cons) was called
    }
    
    "should perform a single column removal" in {
      val (cassandra,client) = clientSetup
      val cp = new thrift.ColumnPath
      cp.column_family = "cf"
      cp.column = "c".getBytes
      val cons = thrift.ConsistencyLevel.ANY
      val timestamp = Clock.timestamp
      
      cassandra.keyspace("ks") { ks =>
        implicit val consistency = WriteConsistency.Any
        ks.remove("row", "cf" -> "c", timestamp)
      }
      
      client.remove("ks", "row", cp, timestamp, cons) was called
    }
    
    "should perform a single supercolumn insert" in {
      val (cassandra,client) = clientSetup
      val cp = new thrift.ColumnPath
      cp.column_family = "cf"
      cp.super_column = "sc".getBytes
      cp.column = "c".getBytes
      val cons = thrift.ConsistencyLevel.ANY
      val timestamp = Clock.timestamp
      
      cassandra.keyspace("ks") { ks =>
        implicit val consistency = WriteConsistency.Any
        ks.insert("row", "cf" -> "sc" -> "c", "value", timestamp)
      }
      
      client.insert("ks", "row", cp, "value".getBytes, timestamp, cons) was called
    }
  }
}