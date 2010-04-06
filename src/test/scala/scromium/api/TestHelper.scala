package scromium.api

import scromium._
import scromium.connection._
import org.specs.mock.Mockito
import org.apache.cassandra.thrift

trait TestHelper extends Mockito {
  def fakeConnectionPool(conn : Connection) : ConnectionPool = {
    return new ConnectionPool {
      def withConnection[T](block : Connection => T) : T = {
        block(conn)
      }
    }
  }
  
  def clientSetup : thrift.Cassandra.Client = {
    val connection = mock[Connection]
    val pool = fakeConnectionPool(connection)
    val client = mock[thrift.Cassandra.Client]
    Keyspace.pool = pool
    connection.client returns client
    client
  }
}