package scromium

object ByteArray {
  def apply(xs : Int*) : Array[Byte] = {
    xs.map({x => x.toByte}).toArray
  }
}

trait TestHelper {
  def setupSchema(cass : Cassandra) {
    val a = cass.admin
    cass.admin { admin =>
      admin.dropKeyspace("Keyspace")
      admin.keyspace("Keyspace") { ks =>
        ks.columnFamily("ColumnFamily") { cf => cf }
        ks.superColumnFamily("SuperColumnFamily")
        admin.create(ks)
      }
    }
  }
}