package scromium.api

import org.apache.cassandra.thrift

trait Container

trait ContainerFactory[+A] {
  def make(cosc : thrift.ColumnOrSuperColumn) : A
}

object ContainerFactory {
  implicit object GetSuperColumnContainerFactory extends ContainerFactory[GetSuperColumn] {
    def make(cosc : thrift.ColumnOrSuperColumn) : GetSuperColumn = {
      new GetSuperColumn(cosc)
    }
  }
  
  implicit object GetColumnContainerFactory extends ContainerFactory[GetColumn] {
    def make(cosc : thrift.ColumnOrSuperColumn) : GetColumn = {
      new GetColumn(cosc)
    }
  }
}