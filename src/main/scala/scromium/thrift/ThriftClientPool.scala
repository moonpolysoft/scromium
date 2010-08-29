package scromium.thrift

import org.apache.commons.pool._
import org.apache.commons.pool.impl._
import scromium.util.JSON
import scromium.util.Log
import java.util.{Timer, TimerTask}


class ThriftClientPool(config : Map[String,Any],
    socketFactory : ThriftSocketFactory, 
    clusterDiscovery : ThriftClusterDiscovery) extends Log {
  
  val seedHost = config("seedHost").asInstanceOf[String]
  val seedPort = config("seedPort").asInstanceOf[Int]
  val maxIdle = config("maxIdle").asInstanceOf[Int]
  val initCapacity = config("initCapacity").asInstanceOf[Int]
  val hosts = clusterDiscovery.hosts(seedHost,seedPort)
  val clientFactory = new ThriftClientFactory(hosts, seedPort, socketFactory)
  
  val hostRefreshTask = new TimerTask {
    override def run {
      try {
        val newHosts = clusterDiscovery.hosts(seedHost, seedPort)
        clientFactory.synchronized {
          clientFactory.hosts = newHosts
        }
      } catch {
        case e : Throwable =>
          error("Caught exception trying to update the endpoint list.", e)
      }
    }
  }
  val hostRefreshTimer = new Timer("host-refresh-timer")
  hostRefreshTimer.schedule(hostRefreshTask, 60000, 60000)
  
  val p = new GenericObjectPool(clientFactory)
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
  
  override def finalize {
    hostRefreshTimer.cancel
  }
}