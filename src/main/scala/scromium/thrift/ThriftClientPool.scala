package scromium.thrift

import org.apache.commons.pool._
import org.apache.commons.pool.impl._
import scromium.util.JSON
import scromium.util.Log


class ThriftClientPool(config : Map[String,Any],
    socketFactory : ThriftSocketFactory, 
    clusterDiscovery : ThriftClusterDiscovery) extends Log {
  
  val seedHost = config("seedHost").asInstanceOf[String]
  val seedPort = config("seedPort").asInstanceOf[Int]
  val maxIdle = config("maxIdle").asInstanceOf[Int]
  val initCapacity = config("initCapacity").asInstanceOf[Int]
  
  val hosts = clusterDiscovery.hosts(seedHost,seedPort)
  val p = new GenericObjectPool(new ThriftClientFactory(hosts, seedPort, socketFactory))
  p.setMaxIdle(maxIdle)
  
  def borrow : ThriftClient = {
    synchronized {
      p.borrowObject.asInstanceOf[ThriftClient]
    }
  }
  
  def returnConnection(conn : ThriftClient) {
    synchronized {
      p.returnObject(conn)
    }
  }
}