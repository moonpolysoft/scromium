package scromium.api

import org.specs._
import org.specs.mock.Mockito
import org.apache.cassandra.thrift
import org.mockito.Matchers._
import scromium._
import scromium.serializers.Serializers._

class ScannerBuilderSpec extends Specification with Mockito with TestHelper {
  "ScannerBuilder" should {
    "execute a scan for a normal column family with only one section returned" in {
      val (cassandra,client) = clientSetup
      val timestamp = System.currentTimeMillis
      val parent = new thrift.ColumnParent
      parent.column_family = "cf"
      val predicate = new thrift.SlicePredicate
      val range = new thrift.KeyRange
      range.start_key = "row00"
      range.end_key = "row20"
      range.count = 100
      
      val list = new java.util.ArrayList[thrift.KeySlice]
      val clist = new java.util.ArrayList[thrift.ColumnOrSuperColumn]
      val container = new thrift.ColumnOrSuperColumn
      container.column = new thrift.Column("name".getBytes, "value".getBytes, timestamp)
      clist.add(container)
      Range(0,8).foreach {i => list.add(new thrift.KeySlice("row0" + i, clist)) }
      
      client.get_range_slices("ks", parent, predicate, range, thrift.ConsistencyLevel.ONE) returns list
      
      val range2 = range.deepCopy
      range2.start_key = "row08"
      
      val list2 = new java.util.ArrayList[thrift.KeySlice]
      Range(10, 18).foreach {i => list2.add(new thrift.KeySlice("row" + i, clist)) }
      
      client.get_range_slices("ks", parent, predicate, range2, thrift.ConsistencyLevel.ONE) returns list2
      
      val range3 = range.deepCopy
      range3.start_key = "row18"
      
      val list3 = new java.util.ArrayList[thrift.KeySlice]
      
      client.get_range_slices("ks", parent, predicate, range3, thrift.ConsistencyLevel.ONE) returns list3
      
      cassandra.keyspace("ks") {ks =>
        implicit val consistency = ReadConsistency.One
        val scanner = ks.scan("cf").keys("row00", "row20")!
        
        var count = 0
        for (row <- scanner) {
          count += 1
          row.columns.head.valueAs[String] must ==(Some("value"))
        }
        count must ==(16)
      }
      
      client.get_range_slices("ks", parent, predicate, range, thrift.ConsistencyLevel.ONE) was called
      client.get_range_slices("ks", parent, predicate, range2, thrift.ConsistencyLevel.ONE) was called
      client.get_range_slices("ks", parent, predicate, range3, thrift.ConsistencyLevel.ONE) was called
    }
  }
}