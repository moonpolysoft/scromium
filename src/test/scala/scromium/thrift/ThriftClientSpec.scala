package scromium.thrift

import scromium._
import scromium.client._
import scromium.serializers.Serializers._
import org.specs._
import org.specs.mock.Mockito
import org.mockito.Matchers._
import scala.collection.JavaConversions._
import scromium.clocks._

class ThriftClientSpec extends Specification with Mockito with TestHelper {
  "ThriftClient" should {
    var cassandra : Cassandra = null
    
    doBefore { cassandra = Cassandra.startTest; setupSchema(cassandra) }
    doAfter { cassandra.teardownTest }
    
    "ColumnFamily" in {
      "execute put and get commands" in {
        cassandra.keyspace("Keyspace") { ks =>
          ks.columnFamily("ColumnFamily") { cf =>
            cf.batch { put =>
              val r = put.row("row1")
              r.insert("c1", "value")
              cf.put(put)
            }

            val column = cf.getColumn("row1", "c1").get
            column.valueAs[String] must beSome("value")
          }
        }
      }
      
      "execute delete commands" in {
        cassandra.keyspace("Keyspace") { ks =>
          ks.columnFamily("ColumnFamily") { cf =>
            cf.batch { put =>
              put.row("row1").insert("c1", "value")
              cf.put(put)
            }
            
            cf.deleteColumn("row1", "c1")
            
            cf.getColumn("row1", "c1") must beNone
          }
        }
      }
      
      "execute batch updates and multiget" in {
        cassandra.keyspace("Keyspace") { ks =>
          ks.columnFamily("ColumnFamily") { cf =>
            cf.batch { put =>
              put.row("row1").insert("c1", "value1").insert("c2", "value2")
              put.row("row2").insert("c3", "value3")
              cf.put(put)
            }
            
            val results = cf.get(cf.selector(List("row1", "row2")).columns(List("c1", "c2", "c3"))).toList
            println("results " + results)
            for (row <- results) {
              if (row.keyAs[String] == Some("row1")) {
                val cols = row.columns.toList
                cols(0).valueAs[String] must beSome("value1")
                cols(1).valueAs[String] must beSome("value2")
              } else {
                row.columns.next.valueAs[String] must beSome("value3")                
              }
            }
          }
        }
      }
    }
    
    "SuperColumnFamily" in {
      "execute put and get commands" in {
        cassandra.keyspace("Keyspace") { ks =>
          ks.superColumnFamily("SuperColumnFamily") { cf =>
            cf.batch { put =>
              put.row("row1").superColumn("sc1").insert("c1", "value")
              cf.put(put)
            }
            
            val sc = cf.getSuperColumn("row1", "sc1").get
            sc.columns.head.nameAs[String] must beSome("c1")
            sc.columns.head.valueAs[String] must beSome("value")
            
            val c = cf.getSubColumn("row1", "sc1", "c1").get
            c.valueAs[String] must beSome("value")
          }
        }
      }
      
      "execute subcolumn delete commands" in {
        cassandra.keyspace("Keyspace") { ks =>
          ks.superColumnFamily("SuperColumnFamily") { cf =>
            cf.batch { put =>
              put.row("row1").superColumn("sc1").insert("c1", "value")
              cf.put(put)
            }
            
            cf.deleteSubColumn("row1", "sc1", "c1")
            cf.getSuperColumn("row1", "sc1") must beNone
            cf.getSubColumn("row1", "sc1", "c1") must beNone
          }
        }
      }
      
      "execute supercolumn delete commands" in {
        cassandra.keyspace("Keyspace") { ks =>
          ks.superColumnFamily("SuperColumnFamily") { cf =>
            cf.batch { put =>
              put.row("row1").superColumn("sc1").insert("c1", "value")
              cf.put(put)
            }
            
            cf.deleteSuperColumn("row1", "sc1")
            cf.getSuperColumn("row1", "sc1") must beNone
            cf.getSubColumn("row1", "sc1", "c1") must beNone
          }
        }
      }
    }
  }
}