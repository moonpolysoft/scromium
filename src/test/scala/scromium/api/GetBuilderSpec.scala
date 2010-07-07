package scromium.api

import org.specs._
import org.specs.mock.Mockito
import org.apache.cassandra.thrift
import org.mockito.Matchers._
import scromium._
import connection._
import serializers.Serializers._

class GetBuilderSpec extends Specification with Mockito with TestHelper {
  "GetBuilder" should {
    "execute a simple column get" in {
      val (cassandra,client) = clientSetup
      val cp = new thrift.ColumnPath
      cp.column_family = "cf"
      cp.column = "c".getBytes
      val cons = thrift.ConsistencyLevel.ONE
      val con = new thrift.ColumnOrSuperColumn
      con.column = new thrift.Column
      client.get("ks", "row", cp, cons) returns con 
      
      cassandra.keyspace("ks") { ks =>
        implicit val consistency = ReadConsistency.One
        ks.get("row", "cf") / "c" !
      }
      
      there was one(client).get("ks", "row", cp, cons)
    }
    
    "execute a supercolumn get" in {
      val (cassandra,client) = clientSetup
      val cp = new thrift.ColumnPath
      cp.column_family = "cf"
      cp.super_column = "c".getBytes
      val cons = thrift.ConsistencyLevel.ONE
      val corsc = new thrift.ColumnOrSuperColumn
      corsc.super_column = new thrift.SuperColumn
      corsc.super_column.columns = new java.util.ArrayList()
      client.get("ks", "row", cp, cons) returns corsc
      
      cassandra.keyspace("ks") { ks =>
        implicit val consistency = ReadConsistency.One
        ks.get("row", "cf") % "c" !
      }
      
      there was one(client).get("ks", "row", cp, cons)
    }
  }
}