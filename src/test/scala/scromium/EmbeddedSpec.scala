package scromium

import org.specs._
import org.apache.cassandra.thrift._
import scromium.serializers.Serializers._
import scromium.api._

class EmbeddedSpec extends Specification {
  var cassandra : Cassandra = null
  
  "EmbeddedCassandraNode" should {
    doBefore { cassandra = Cassandra.startTest }
    doAfter { cassandra.teardownTest }
    
    "start up and accept writes and reads" in {
      cassandra.keyspace("Keyspace1") { ks =>
        implicit val writeC = WriteConsistency.One
        implicit val readC = ReadConsistency.One
        
        ks.insert("row", "Standard1" -> "column_name", "fuck you")
        
        val result = ks.get("row", "Standard1") / "column_name"!
        
        result.valueAs[String] must ==(Some("fuck you"))
      }
    }
    
    "teardown the old crap" in {
      cassandra.keyspace("Keyspace1") { ks =>
        implicit val writeC = WriteConsistency.One
        implicit val readC = ReadConsistency.One
        
/*        ks.insert("row", "Standard1" -> "column_name", "fuck you")*/
        
        ({ ks.get("row", "Standard1") / "column_name"! }) must throwA[NotFoundException]
        
      }
    }
  }
}