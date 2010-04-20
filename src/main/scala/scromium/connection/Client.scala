package scromium.connection

import org.apache.cassandra.thrift._
import java.util.List
import java.util.Map

abstract class Client {
  
  def get(ks : String, k : String, cp : ColumnPath, c : ConsistencyLevel) : ColumnOrSuperColumn = {
    throw new Exception("not implemented")
  }
  
  def multiget_slice(ks : String, keys : List[String], cp : ColumnParent, p : SlicePredicate, c : ConsistencyLevel) : Map[String,List[ColumnOrSuperColumn]] = {
    throw new Exception("not implemented")
  }
  
  def get_range_slices(ks : String, cp : ColumnParent, p : SlicePredicate, kr : KeyRange, c : ConsistencyLevel) : List[KeySlice] = {
    throw new Exception("not implemented")
  }
  
  def insert(ks : String, key : String, cp : ColumnPath, value : Array[Byte], timestamp : Long, c : ConsistencyLevel) {
    throw new Exception("not implemented")
  }
  
  def remove(ks : String, key : String, cp : ColumnPath, timestamp : Long, c : ConsistencyLevel) {
    throw new Exception("not implemented")
  }
  
  def batch_mutate(ks : String, map : Map[String,Map[String,List[Mutation]]], c : ConsistencyLevel) {
    throw new Exception("not implemented")
  }
}