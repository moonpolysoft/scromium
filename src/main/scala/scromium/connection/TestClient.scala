package scromium.connection

import org.apache.cassandra.thrift._
import java.util.{List => JList,Map => JMap}

class TestClient(iface : Cassandra.Iface) extends Client {
  type SliceMap = JMap[String,JList[ColumnOrSuperColumn]]
  type MuteMap = JMap[String,JMap[String,JList[Mutation]]]
  
  override def get(ks : String, k : String, cp : ColumnPath, c : ConsistencyLevel) : ColumnOrSuperColumn = {
    iface.get(ks,k,cp,c)
  }
  
  override def multiget_slice(ks : String, keys : JList[String], cp : ColumnParent, p : SlicePredicate, c : ConsistencyLevel) : SliceMap = {
    iface.multiget_slice(ks,keys,cp,p,c)
  }
  
  override def get_range_slices(ks : String, cp : ColumnParent, p : SlicePredicate, kr : KeyRange, c : ConsistencyLevel) : JList[KeySlice] = {
    iface.get_range_slices(ks,cp,p,kr,c)
  }
  
  override def insert(ks : String, key : String, cp : ColumnPath, value : Array[Byte], timestamp : Long, c : ConsistencyLevel) {
    iface.insert(ks,key,cp,value,timestamp,c)
  }
  
  override def batch_mutate(ks : String, map : MuteMap, c : ConsistencyLevel) {
    iface.batch_mutate(ks,map,c)
  }
}