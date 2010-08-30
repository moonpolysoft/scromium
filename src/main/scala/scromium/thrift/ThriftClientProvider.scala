package scromium.thrift

import scromium.client._
import scromium.util.Log

class ThriftClientProvider(options : Map[String, String]) extends ClientProvider {
  val provider = new WrappedProvider(new ThriftClientPool(options,
    new ThriftSocketFactory,
    new ThriftClusterDiscovery))
  
  def withClient[T](block : Client => T) : T = provider.withClient(block)
}

class WrappedProvider(objectPool : ThriftClientPool) extends ClientProvider with Log {
  
  def withClient[T](block : Client => T) : T = {
    var connection : ThriftClient = null
    try {
      connection = objectPool.borrow
      block(connection)
    } catch {
      case ex : Throwable =>
        error("Error while trying to use connection", ex)
        throw ex
    } finally {
      if (connection != null) objectPool.returnConnection(connection)
    }
  }
}