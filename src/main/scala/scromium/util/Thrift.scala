package scromium.util

import org.apache.cassandra.thrift._
import scala.collection.JavaConversions._

object Thrift {
  def columnContainer(c : Array[Byte], value : Array[Byte], timestamp : Long) : ColumnOrSuperColumn = {
    val container = new ColumnOrSuperColumn
    val column = new Column(c,value,new Clock(timestamp))
    container.column = column
    container
  }
  
  def superColumnContainer(sc : Array[Byte], columns : (Array[Byte], Array[Byte], Long)*) : ColumnOrSuperColumn = {
    val container = new ColumnOrSuperColumn
    val superColumn = new SuperColumn(sc, new java.util.ArrayList[Column])
    columns.foreach { case (c,v,t) =>
      val column = new Column(c,v,new Clock(t))
      superColumn.columns.add(column)
    }
    container.super_column = superColumn
    container
  }
  
  def columnMutation(c : Array[Byte], value : Array[Byte], timestamp : Long) : Mutation = {
    val mutation = new Mutation
    val container = columnContainer(c, value, timestamp)
    mutation.column_or_supercolumn = container
    mutation
  }
  
  def superColumnMutation(sc : Array[Byte], columns : (Array[Byte], Array[Byte], Long)*) : Mutation = {
    val mutation = new Mutation
    val container = superColumnContainer(sc,columns : _*)
    mutation.column_or_supercolumn = container
    mutation
  }
  
  def keyRange(startKey : Array[Byte], endKey : Array[Byte], count : Int) : KeyRange = {
    val kr = new KeyRange
    kr.start_key = startKey
    kr.end_key = endKey
    kr.count = count
    kr
  }
  
  def sliceRange(start : Array[Byte], end : Array[Byte], count : Int) : SliceRange = {
    val r = new SliceRange
    r.start = start
    r.finish = end
    r.count = count
    r
  }
  
  def slicePredicate(start : Array[Byte], end : Array[Byte], count : Int) : SlicePredicate = {
    val p = new SlicePredicate
    p.slice_range = sliceRange(start, end, count)
    p
  }
  
  def slicePredicate(columns : Array[Byte]*) : SlicePredicate = {
    val p = new SlicePredicate
    p.column_names = new java.util.ArrayList[Array[Byte]]
    columns.foreach { c => p.column_names.add(c) }
    p
  }
}