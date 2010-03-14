package scromium.api

import scromium.connection._

trait TestHelper {
  def fakeConnectionPool(conn : Connection) : ConnectionPool = {
    return new ConnectionPool {
      def withConnection[T](block : Connection => T) : T = {
        block(conn)
      }
      
      def borrow = conn
      def returnConnection(conn : Connection) {}
    }
  }
}