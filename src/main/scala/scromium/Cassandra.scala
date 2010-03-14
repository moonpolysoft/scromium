package scromium

import connection._

object Cassandra {
  def start {
    val pool = ConnectionPool.createConnectionPool
    Keyspace.pool = pool
  }
}