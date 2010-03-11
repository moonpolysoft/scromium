package scromium.connection

import org.apache.commons.pool._
import org.apache.commons.pool.impl._

class ConnectionPool(val seedHost : String, 
    val seedPort : Int, 
    val maxIdle : Int, 
    val initCapacity : Int, 
    socketFactory : SocketFactory = new SocketFactory, 
    clusterDiscovery : ClusterDiscovery = new ClusterDiscovery) {
  
  val hosts = clusterDiscovery.hosts(seedHost,seedPort)
  val objectPool = new StackObjectPool(new ConnectionFactory(hosts, seedPort, socketFactory), maxIdle, initCapacity)
  
  def borrow : Connection = {
    objectPool.borrowObject.asInstanceOf[Connection]
  }
  
  def returnConnection(conn : Connection) {
    objectPool.returnObject(conn)
  }
}
