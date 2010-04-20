package scromium.connection

import org.apache.commons.pool._
import org.apache.commons.pool.impl._
import scromium.util.JSON
import scromium.util.Log


class CommonsConnectionPool(config : Map[String,Any],
    socketFactory : SocketFactory = new SocketFactory, 
    clusterDiscovery : ClusterDiscovery = new ClusterDiscovery) extends ConnectionPool with Log {
  
  val seedHost = config("seedHost").asInstanceOf[String]
  val seedPort = config("seedPort").asInstanceOf[Int]
  val maxIdle = config("maxIdle").asInstanceOf[Int]
  val initCapacity = config("initCapacity").asInstanceOf[Int]
  
  val hosts = clusterDiscovery.hosts(seedHost,seedPort)
  val objectPool = new StackObjectPool(new ConnectionFactory(hosts, seedPort, socketFactory), maxIdle, initCapacity)
  
  def withConnection[T](block : Client => T) : T = {
    var connection : Connection = null
    try {
      connection = borrow
      block(connection)
    } catch {
      case ex : Throwable =>
        error("Error while trying to use connection", ex)
        throw ex
    } finally {
      if (connection != null) returnConnection(connection)
    }
  }
  
  def borrow : Connection = {
    synchronized {
      objectPool.borrowObject.asInstanceOf[Connection]
    }
  }
  
  def returnConnection(conn : Connection) {
    synchronized {
      objectPool.returnObject(conn)
    }
  }
}