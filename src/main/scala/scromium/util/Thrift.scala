package scromium.util

import org.apache.cassandra.thrift._

object Thrift {
  def columnContainer(c : Array[Byte], value : Array[Byte], timestamp : Long) : ColumnOrSuperColumn = {
    val container = new ColumnOrSuperColumn
    val column = new Column(c,value,timestamp)
    container.column = column
    container
  }
  
  def superColumnContainer(sc : Array[Byte], columns : (Array[Byte], Array[Byte], Long)*) : ColumnOrSuperColumn = {
    val container = new ColumnOrSuperColumn
    val superColumn = new SuperColumn(sc, new java.util.ArrayList[Column])
    columns.foreach { case (c,v,t) =>
      val column = new Column(c,v,t)
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
}