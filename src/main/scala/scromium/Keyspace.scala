package scromium

import api._
import connection.ConnectionPool

object Keyspace {
  var pool : ConnectionPool = null
  
  def apply(ksName : String)(block : Keyspace => Any) : Any = {
    if (pool == null) {
      throw new Exception("Cassandra client needs to be started first.")
    }
    val ks = new Keyspace(ksName, pool)
    block(ks)
  }
}

case class Keyspace(name : String, pool : ConnectionPool) {
  def get(row : String, cf : String) = new CFPath(this, row, cf)
}
