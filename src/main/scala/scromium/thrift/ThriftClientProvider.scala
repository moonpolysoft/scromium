package scromium.thrift

import scromium.client._
import scromium.util.Log

class ThriftClientProvider(objectPool : ThriftClientPool) extends ClientProvider with Log {
  
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