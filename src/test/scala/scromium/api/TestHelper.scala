package scromium.api

import scromium._
import scromium.connection._
import org.specs.mock.Mockito
import org.apache.cassandra.thrift

trait TestHelper extends Mockito {
  def fakeConnectionPool(conn : Connection) : ConnectionPool = {
    return new ConnectionPool {
      def withConnection[T](block : Client => T) : T = {
        block(conn)
      }
    }
  }
  
  def clientSetup : (Cassandra, Connection) = {
    val connection = mock[Connection]
    val pool = fakeConnectionPool(connection)
    (new Cassandra(pool), connection)
  }
}