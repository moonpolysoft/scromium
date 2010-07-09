package scromium.connection

import com.yammer.jmx._
import com.yammer.metrics._
import org.apache.cassandra.thrift._
import java.util.concurrent.TimeUnit
import java.util.List
import java.util.Map

object ClientStats extends JmxManaged {
  val getTimer = new Timer
  val multigetSliceTimer = new Timer
  val getRangeSlicesTimer = new Timer
  val insertTimer = new Timer
  val removeTimer = new Timer
  val batchMutateTimer = new Timer
  
  val getLoad = new LoadMeter
  val multigetSliceLoad = new LoadMeter
  val getRangeSlicesLoad = new LoadMeter
  val insertLoad = new LoadMeter
  val removeLoad = new LoadMeter
  val batchMutateLoad = new LoadMeter
  
  enableJMX("Cassandra") { jmx => 
    jmx.addMeter("gets", getLoad, TimeUnit.SECONDS)
    jmx.addMeter("multigets", multigetSliceLoad, TimeUnit.SECONDS)
    jmx.addMeter("get_range_slices", getRangeSlicesLoad, TimeUnit.SECONDS)
    jmx.addMeter("inserts", insertLoad, TimeUnit.SECONDS)
    jmx.addMeter("removes", removeLoad, TimeUnit.SECONDS)
    jmx.addMeter("batch_mutates", batchMutateLoad, TimeUnit.SECONDS)
    
    jmx.addTimer("get_latency", getTimer, TimeUnit.MILLISECONDS)
    jmx.addTimer("multiget_latency", multigetSliceTimer, TimeUnit.MILLISECONDS)
    jmx.addTimer("get_range_slices_latency", getRangeSlicesTimer, TimeUnit.MILLISECONDS)
    jmx.addTimer("insert_latency", insertTimer, TimeUnit.MILLISECONDS)
    jmx.addTimer("remove_latency", removeTimer, TimeUnit.MILLISECONDS)
    jmx.addTimer("batch_mutate_latency", batchMutateTimer, TimeUnit.MILLISECONDS)
  }
}

import ClientStats._

abstract class Client {
  def inst_get(ks : String, k : String, cp : ColumnPath, c : ConsistencyLevel) : ColumnOrSuperColumn
  def inst_multiget_slice(ks : String, keys : List[String], cp : ColumnParent, p : SlicePredicate, c : ConsistencyLevel) : Map[String,List[ColumnOrSuperColumn]]
  def inst_get_range_slices(ks : String, cp : ColumnParent, p : SlicePredicate, kr : KeyRange, c : ConsistencyLevel) : List[KeySlice]
  def inst_insert(ks : String, key : String, cp : ColumnPath, value : Array[Byte], timestamp : Long, c : ConsistencyLevel)
  def inst_remove(ks : String, key : String, cp : ColumnPath, timestamp : Long, c : ConsistencyLevel)
  def inst_batch_mutate(ks : String, map : Map[String,Map[String,List[Mutation]]], c : ConsistencyLevel)
  
  def get(ks : String, k : String, cp : ColumnPath, c : ConsistencyLevel) : ColumnOrSuperColumn = {
    getLoad.mark(1)
    getTimer.time { inst_get(ks, k, cp, c) }
  }
  
  def multiget_slice(ks : String, keys : List[String], cp : ColumnParent, p : SlicePredicate, c : ConsistencyLevel) : Map[String,List[ColumnOrSuperColumn]] = {
    multigetSliceLoad.mark(1)
    multigetSliceTimer.time { inst_multiget_slice(ks, keys, cp, p, c) }
  }
  
  def get_range_slices(ks : String, cp : ColumnParent, p : SlicePredicate, kr : KeyRange, c : ConsistencyLevel) : List[KeySlice] = {
    getRangeSlicesLoad.mark(1)
    getRangeSlicesTimer.time { inst_get_range_slices(ks, cp, p, kr, c) }
  }
  
  def insert(ks : String, key : String, cp : ColumnPath, value : Array[Byte], timestamp : Long, c : ConsistencyLevel) {
    insertLoad.mark(1)
    insertTimer.time { inst_insert(ks, key, cp, value, timestamp, c) }
  }
  
  def remove(ks : String, key : String, cp : ColumnPath, timestamp : Long, c : ConsistencyLevel) {
    removeLoad.mark(1)
    removeTimer.time { inst_remove(ks, key, cp, timestamp, c) }
  }
  
  def batch_mutate(ks : String, map : Map[String,Map[String,List[Mutation]]], c : ConsistencyLevel) {
    batchMutateLoad.mark(1)
    batchMutateTimer.time { inst_batch_mutate(ks, map, c) }
  }
}