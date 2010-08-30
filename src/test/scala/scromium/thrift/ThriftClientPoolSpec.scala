package scromium.thrift

import org.specs._
import org.specs.mock.Mockito
import org.mockito.Matchers._
import org.apache.thrift.transport.{TSocket, TTransportException}
import java.io.IOException

class ThriftClientPoolSpec extends Specification with Mockito {
  "CommonsConnectionPool" should {
    "create a simple connection" in {
      val socketFactory = mock[ThriftSocketFactory]
      val socket = mock[TSocket]
      val clusterDiscovery = mock[ThriftClusterDiscovery]
      
      clusterDiscovery.hosts("10.10.10.10", 9160) returns List("127.0.0.1")
      socketFactory.make("127.0.0.1", 9160) returns socket
      socket.isOpen returns true
      
      val connPool = new ThriftClientPool(Map("seedHost" -> "10.10.10.10", "seedPort" -> 9160, "maxIdle" -> 1, "initCapacity" -> 1), socketFactory, clusterDiscovery)
      val connection = connPool.borrow
      
      connection must notBeNull
      connection.asInstanceOf[ThriftConnection].isOpen must beTrue
    }
    
    "handle down hosts" in {
      val socketFactory = mock[ThriftSocketFactory]
      val socket = mock[TSocket]
      val clusterDiscovery = mock[ThriftClusterDiscovery]
      
      clusterDiscovery.hosts("10.10.10.10", 9160) returns List("127.0.0.1", "192.168.0.1")
      socketFactory.make("127.0.0.1", 9160) throws new TTransportException("")
      socketFactory.make("192.168.0.1", 9160) returns socket
      socket.isOpen returns true
      
      val connPool = new ThriftClientPool(Map("seedHost" -> "10.10.10.10", "seedPort" -> 9160, "maxIdle" -> 1, "initCapacity" -> 1), socketFactory, clusterDiscovery)
      val connection = connPool.borrow
      
      connection must notBeNull
      connection.asInstanceOf[ThriftConnection].isOpen must beTrue
    }
    
    "throw an error if all hosts are down" in {
      val socketFactory = mock[ThriftSocketFactory]
      val clusterDiscovery = mock[ThriftClusterDiscovery]
      
      clusterDiscovery.hosts("10.10.10.10", 9160) returns List("127.0.0.1")
      socketFactory.make("127.0.0.1", 9160) throws new TTransportException("")
      
      val connPool = new ThriftClientPool(Map("seedHost" -> "10.10.10.10", "seedPort" -> 9160, "maxIdle" -> 1, "initCapacity" -> 1), socketFactory, clusterDiscovery)
      connPool must notBeNull
      val a = () => { connPool.borrow } 
      a() must throwA[Exception]
    }
    
    "cycle hosts" in {
      val socketFactory = mock[ThriftSocketFactory]
      val socket = mock[TSocket]
      val clusterDiscovery = mock[ThriftClusterDiscovery]
      
      clusterDiscovery.hosts("10.10.10.10", 9160) returns List("127.0.0.1", "192.168.0.1")
      socket.isOpen returns true
      socketFactory.make(anyString, anyInt) returns socket
      
      val connPool = new ThriftClientPool(Map("seedHost" -> "10.10.10.10", "seedPort" -> 9160, "maxIdle" -> 10, "initCapacity" -> 10), socketFactory, clusterDiscovery)
      connPool.borrow
      connPool.borrow
      connPool.borrow
      
      there was one(socketFactory).make("127.0.0.1", 9160) then
      one(socketFactory).make("192.168.0.1", 9160) then
      one(socketFactory).make("127.0.0.1", 9160)
      
    }
  }
}