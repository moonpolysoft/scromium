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

            val selector = cf.selector("row1").column("c1")
            val column = cf.getColumn("row1", "c1").get
            column.valueAs[String] must beSome("value")
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
    }
  }
}