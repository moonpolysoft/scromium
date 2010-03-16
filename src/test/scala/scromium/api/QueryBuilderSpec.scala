package scromium.api

import org.specs._
import org.specs.mock.Mockito
import org.apache.cassandra.thrift
import org.mockito.Matchers._
import scromium._
import connection._
import serializers.Serializers._

class QueryBuilderSpec extends Specification with Mockito with TestHelper {
  "QueryBuilder" should {
    "execute get_range_slices for a normal column family and only a key range" in {
      val client = clientSetup
      val timestamp = System.currentTimeMillis
      val parent = new thrift.ColumnParent
      parent.column_family = "cf"
      val predicate = new thrift.SlicePredicate
      val range = new thrift.KeyRange
      range.start_key = "start"
      range.end_key = "finish"
      range.count = 100
      
      val list = new java.util.ArrayList[thrift.KeySlice]
      val clist = new java.util.ArrayList[thrift.ColumnOrSuperColumn]
      val container = new thrift.ColumnOrSuperColumn
      container.column = new thrift.Column("name".getBytes, "value".getBytes, timestamp)
      clist.add(container)
      list.add(new thrift.KeySlice("row", clist))
      
      client.get_range_slices("ks", parent, predicate, range, thrift.ConsistencyLevel.ONE) returns list 
      
      Keyspace("ks") {ks =>
        implicit val consistency = ReadConsistency.One
        val results = ks.rangeSlices("cf").keys("start", "finish")!
      }
      
      client.get_range_slices("ks", parent, predicate, range, thrift.ConsistencyLevel.ONE) was called
    }
  }
}