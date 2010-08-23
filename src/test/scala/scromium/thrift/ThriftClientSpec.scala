package scromium.thrift

import scromium._
import scromium.client._
import scromium.serializers.Serializers._
import org.specs._
import org.specs.mock.Mockito
import org.mockito.Matchers._
import scala.collection.JavaConversions._
import scromium.clocks._

class ThriftClientSpec extends Specification with Mockito {
  "ThriftClient" should {
    var cassandra : Cassandra = null
    val clock = MicrosecondEpochClock
    
    doBefore { cassandra = Cassandra.startTest }
    doAfter { cassandra.teardownTest }
    
    "execute put and get commands" in {
      val put = new Put(clock)
      put.row("row").insert("col", 5)
      
    }
  }
}