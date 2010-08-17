/*package scromium.connection

import scromium.util.JSON
import scromium.util.Log
import java.util.List
import java.net.InetAddress
import java.util.{Map => JMap}
import java.util.{HashSet => JSet}
import scala.collection.JavaConversions._
import org.apache.cassandra.service.StorageService
import org.apache.cassandra.config.DatabaseDescriptor
import org.apache.cassandra.thrift._

class Scrotorboat(config : Map[String,Any],
    socketFactory : SocketFactory = new SocketFactory, 
    clusterDiscovery : ClusterDiscovery = new ClusterDiscovery) extends ConnectionPool with Log {
  
  val seedHost = config("seedHost").asInstanceOf[String]
  DatabaseDescriptor.setSeeds(new JSet(Set(InetAddress.getByName(seedHost))))

  StorageService.instance.initClient
  val client = new ScrotorboatClient

  def withConnection[T](block : Client => T) : T = {
    block(client)
  }
}

class ScrotorboatClient extends Client {
  val client = new CassandraServer
  
  override def inst_get(ks : String, k : String, cp : ColumnPath, c : ConsistencyLevel) : ColumnOrSuperColumn = {
    client.get(ks, k, cp, c)
  }
  
  override def inst_multiget_slice(ks : String, keys : List[String], cp : ColumnParent, p : SlicePredicate, c : ConsistencyLevel) : JMap[String,List[ColumnOrSuperColumn]] = {
    client.multiget_slice(ks, keys, cp, p, c)
  }
  
  override def inst_get_range_slices(ks : String, cp : ColumnParent, p : SlicePredicate, kr : KeyRange, c : ConsistencyLevel) : List[KeySlice] = {
    client.get_range_slices(ks, cp, p, kr, c)
  }
  
  override def inst_insert(ks : String, key : String, cp : ColumnPath, value : Array[Byte], timestamp : Long, c : ConsistencyLevel) {
    client.insert(ks, key, cp, value, timestamp, c)
  }
  
  override def inst_remove(ks : String, key : String, cp : ColumnPath, timestamp : Long, c : ConsistencyLevel) {
    client.remove(ks, key, cp, timestamp, c)
  }
  
  override def inst_batch_mutate(ks : String, map : JMap[String,JMap[String,List[Mutation]]], c : ConsistencyLevel) {
    client.batch_mutate(ks, map, c)
  }
}*/