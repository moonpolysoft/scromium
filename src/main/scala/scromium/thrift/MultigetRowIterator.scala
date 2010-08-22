package scromium.thrift

import scromium._
import java.util.{Map => JMap, List => JList}
import scala.collection.JavaConversions._
import org.apache.cassandra.thrift.ColumnOrSuperColumn

abstract class MultigetRowIterator[C <: Columnar](map : JMap[Array[Byte],JList[ColumnOrSuperColumn]]) extends RowIterator[C] {
  
  val mapIterator = map.entrySet.iterator
  
  def hasNext() : Boolean = {
    mapIterator.hasNext()
  }
  
  def next() : Row[C] = {
    val entry = mapIterator.next
    val containers = entry.getValue
    val columns = containers.map(unpackColumn(_))
    new MultigetRow(entry.getKey, columns.toList)
  }
  
  def unpackColumn(container : ColumnOrSuperColumn) : C
}

class MGColumnRowIterator(map : JMap[Array[Byte],JList[ColumnOrSuperColumn]]) extends MultigetRowIterator[Column](map) {
  def unpackColumn(c : ColumnOrSuperColumn) : Column = {
    Thrift.unpackColumn(c)
  }
}

class MGSuperColumnRowIterator(map : JMap[Array[Byte],JList[ColumnOrSuperColumn]]) extends MultigetRowIterator[SuperColumn](map) {
  def unpackColumn(c : ColumnOrSuperColumn) : SuperColumn = {
    Thrift.unpackSuperColumn(c)
  }
}