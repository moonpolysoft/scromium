package scromium.connection

import org.specs._
import org.specs.mock.Mockito
import org.mockito.Matchers._
import org.apache.thrift.transport.{TSocket, TTransportException}
import java.io.IOException

class ConnectionPoolSpec extends Specification with Mockito {
  "CommonsConnectionPool" should {
    "create a simple connection" in {
      val socketFactory = mock[SocketFactory]
      val socket = mock[TSocket]
      val clusterDiscovery = mock[ClusterDiscovery]
      
      clusterDiscovery.hosts("10.10.10.10", 9160) returns List("127.0.0.1")
      socketFactory.make("127.0.0.1", 9160) returns socket
      socket.isOpen returns true
      
      val connPool = new CommonsConnectionPool(Map("seedHost" -> "10.10.10.10", "seedPort" -> 9160, "maxIdle" -> 1, "initCapacity" -> 1), socketFactory, clusterDiscovery)
      val connection = connPool.borrow
      
      connection must notBeNull
      connection.isOpen must beTrue
    }
    
    "handle down hosts" in {
      val socketFactory = mock[SocketFactory]
      val socket = mock[TSocket]
      val clusterDiscovery = mock[ClusterDiscovery]
      
      clusterDiscovery.hosts("10.10.10.10", 9160) returns List("127.0.0.1", "192.168.0.1")
      socketFactory.make("127.0.0.1", 9160) throws new TTransportException("")
      socketFactory.make("192.168.0.1", 9160) returns socket
      socket.isOpen returns true
      
      val connPool = new CommonsConnectionPool(Map("seedHost" -> "10.10.10.10", "seedPort" -> 9160, "maxIdle" -> 1, "initCapacity" -> 1), socketFactory, clusterDiscovery)
      val connection = connPool.borrow
      
      connection must notBeNull
      connection.isOpen must beTrue
    }
    
    "throw an error if all hosts are down" in {
      val socketFactory = mock[SocketFactory]
      val clusterDiscovery = mock[ClusterDiscovery]
      
      clusterDiscovery.hosts("10.10.10.10", 9160) returns List("127.0.0.1")
      socketFactory.make("127.0.0.1", 9160) throws new TTransportException("")
      
      val connPool = new CommonsConnectionPool(Map("seedHost" -> "10.10.10.10", "seedPort" -> 9160, "maxIdle" -> 1, "initCapacity" -> 1), socketFactory, clusterDiscovery)
      connPool must notBeNull
      val a = () => { connPool.borrow } 
      a() must throwA[Exception]
    }
    
    "cycle hosts" in {
      val socketFactory = mock[SocketFactory]
      val socket = mock[TSocket]
      val clusterDiscovery = mock[ClusterDiscovery]
      
      clusterDiscovery.hosts("10.10.10.10", 9160) returns List("127.0.0.1", "192.168.0.1")
      socket.isOpen returns true
      socketFactory.make(anyString, anyInt) returns socket
      
      val connPool = new CommonsConnectionPool(Map("seedHost" -> "10.10.10.10", "seedPort" -> 9160, "maxIdle" -> 10, "initCapacity" -> 10), socketFactory, clusterDiscovery)
      connPool.borrow
      connPool.borrow
      connPool.borrow
      
      there was one(socketFactory).make("127.0.0.1", 9160) then
      one(socketFactory).make("192.168.0.1", 9160) then
      one(socketFactory).make("127.0.0.1", 9160)
      
    }
  }
  
  "ActorConnectionPool" should {
    "create a simple connection" in {
      val socketFactory = mock[SocketFactory]
      val socket = mock[TSocket]
      val clusterDiscovery = mock[ClusterDiscovery]
      
      clusterDiscovery.hosts("10.10.10.10", 9160) returns List("127.0.0.1")
      socketFactory.make("127.0.0.1", 9160) returns socket
      socket.isOpen returns true
      
      val connPool = new ActorConnectionPool(Map("seedHost" -> "10.10.10.10", "seedPort" -> 9160, "actors" -> 1), socketFactory, clusterDiscovery)
      val connection = connPool.withConnection { c => c}
      
      
      connection must notBeNull
      connection.asInstanceOf[Connection].isOpen must beTrue
    }
    
    "handle down hosts" in {
      val socketFactory = mock[SocketFactory]
      val socket = mock[TSocket]
      val clusterDiscovery = mock[ClusterDiscovery]
      
      clusterDiscovery.hosts("10.10.10.10", 9160) returns List("127.0.0.1", "192.168.0.1")
      socketFactory.make("127.0.0.1", 9160) throws new TTransportException("")
      socketFactory.make("192.168.0.1", 9160) returns socket
      socket.isOpen returns true
      
      val connPool = new ActorConnectionPool(Map("seedHost" -> "10.10.10.10", "seedPort" -> 9160, "actors" -> 1), socketFactory, clusterDiscovery)
      var connection = connPool.withConnection { c => c }
      
      connection must notBeNull
      connection.asInstanceOf[Connection].isOpen must beTrue
    }
    
    "throw an error if all hosts are down" in {
      val socketFactory = mock[SocketFactory]
      val clusterDiscovery = mock[ClusterDiscovery]
      
      clusterDiscovery.hosts("10.10.10.10", 9160) returns List("127.0.0.1")
      socketFactory.make("127.0.0.1", 9160) throws new TTransportException("")
      
      val connPool = new ActorConnectionPool(Map("seedHost" -> "10.10.10.10", "seedPort" -> 9160, "actors" -> 1), socketFactory, clusterDiscovery)
      connPool must notBeNull
      val a = () => { connPool.withConnection {c => c} } 
      a() must throwA[Exception]
    }
    
    "cycle hosts" in {
      val socketFactory = mock[SocketFactory]
      val socket = mock[TSocket]
      val clusterDiscovery = mock[ClusterDiscovery]
      
      clusterDiscovery.hosts("10.10.10.10", 9160) returns List("127.0.0.1", "192.168.0.1")
      socket.isOpen returns true
      socketFactory.make(anyString, anyInt) returns socket
      
      val connPool = new ActorConnectionPool(Map("seedHost" -> "10.10.10.10", "seedPort" -> 9160, "actors" -> 3), socketFactory, clusterDiscovery)
      
      Thread.sleep(50)
      
      there was one(socketFactory).make("127.0.0.1", 9160) then
      one(socketFactory).make("192.168.0.1", 9160) then
      one(socketFactory).make("127.0.0.1", 9160)
    }
    
    "survive host outages" in {
      val socketFactory = mock[SocketFactory]
      val badSocket = mock[TSocket]
      val goodSocket = mock[TSocket]
      val clusterDiscovery = mock[ClusterDiscovery]
      
      clusterDiscovery.hosts("10.10.10.10", 9160) returns List("127.0.0.1", "192.168.0.1")
      socketFactory.make("127.0.0.1", 9160) returns badSocket
      socketFactory.make("192.168.0.1", 9160) returns goodSocket
      
      badSocket.flush throws new TTransportException("fuck right off")
      
      val connPool = new ActorConnectionPool(Map("seedHost" -> "10.10.10.10", "seedPort" -> 9160, "actors" -> 1), socketFactory, clusterDiscovery)
      
      connPool.withConnection { conn => 
        conn.asInstanceOf[Connection].socket.flush
      }
      
      there was one(badSocket).flush
      there was one(goodSocket).flush
    }
  }
}