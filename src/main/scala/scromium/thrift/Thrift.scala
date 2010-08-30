package scromium.thrift

import scromium.client.{Delete, Read}
import org.apache.cassandra.thrift._
import scala.collection.JavaConversions._
import scromium.meta._

object Thrift {
  def column(c : scromium.Column) : Column = {
    val column = new Column(c.name, c.value, new Clock(c.timestamp))
    for (ttl <- c.ttl) column.ttl = ttl
    column
  }
  
  def columnContainer(c : scromium.Column) : ColumnOrSuperColumn = {
    val container = new ColumnOrSuperColumn
    container.column = column(c)
    container
  }
  
  def superColumn(sc : scromium.SuperColumn) : SuperColumn = {
    new SuperColumn(sc.name, sc.columns.map(column(_)))
  }
  
  def superColumnContainer(sc : scromium.SuperColumn) : ColumnOrSuperColumn = {
    val container = new ColumnOrSuperColumn
    container.super_column = superColumn(sc)
    container
  }
  
  def unpackColumn(corsc : ColumnOrSuperColumn) : scromium.Column = {
    column(corsc.column)
  }
  
  def column(c : Column) : scromium.Column = {
    val ttl = if (c.isSetTtl)
      Some(c.ttl)
    else
      None
    scromium.Column(c.name, c.value, c.clock.timestamp, ttl)
  }
  
  def unpackSuperColumn(corsc : ColumnOrSuperColumn) : scromium.SuperColumn = {
    println("container " + corsc)
    superColumn(corsc.super_column)
  }
  
  def superColumn(sc : SuperColumn) : scromium.SuperColumn = {
    println("sc " + sc)
    scromium.SuperColumn(sc.name, sc.columns.map(column(_)).toList)
  }
  
  def columnMutation(c : scromium.Column) : Mutation = {
    val mutation = new Mutation
    mutation.column_or_supercolumn = columnContainer(c)
    mutation
  }
  
  def superColumnMutation(sc : scromium.SuperColumn) : Mutation = {
    val mutation = new Mutation
    mutation.column_or_supercolumn = superColumnContainer(sc)
    mutation
  }
  
  def slicePredicate(columns : List[Array[Byte]]) : SlicePredicate = {
    val predicate = new SlicePredicate
    predicate.column_names = columns
    predicate
  }
  
  def slicePredicate(slice : scromium.Slice) : SlicePredicate = {
    val predicate = new SlicePredicate
    val sliceRange = new SliceRange(slice.start, slice.end, slice.reversed, slice.limit.get)
    predicate.slice_range = sliceRange
    predicate
  }
  
  def deleteMutation(d : Delete) : Mutation = {
    val mutation = new Mutation
    mutation.deletion = deletion(d)
    mutation
  }
  
  def deletion(d : Delete) : Deletion = {
    val deletion = new Deletion(new Clock(d.clock.timestamp))
    d match {
      case Delete(keys, cf, Some(List(column)), Some(subColumns), _, _) =>
        deletion.super_column = column
        deletion.predicate = slicePredicate(subColumns)
      case Delete(keys, cf, Some(List(column)), None, Some(slice), _) =>
        deletion.super_column = column
        deletion.predicate = slicePredicate(slice)
      case Delete(keys, cf, Some(columns), None, None, _) =>
        deletion.predicate = slicePredicate(columns)
      case Delete(keys, cf, _, _, Some(slice), _) =>
        deletion.predicate = slicePredicate(slice)
      case _ =>
        throw new Exception("incomplete delete command: " + d)
    }
    deletion
  }
  
  def readToColumnParent(r : Read) : ColumnParent = {
    val columnParent = new ColumnParent(r.columnFamily)
    r match {
      case Read(_, _, Some(List(super_column)), Some(subc :: tail), _) =>
        columnParent.super_column = super_column
      case _ =>
        Unit
    }
    columnParent
  }
  
  def readToPredicate(r : Read) : SlicePredicate = r match {
    case Read(_, _, _, Some(subColumns), _) =>
      slicePredicate(subColumns)
    case Read(_, _, _, _, Some(slice)) =>
      slicePredicate(slice)
    case Read(_, _, Some(columns), _, _) =>
      slicePredicate(columns)
    case _ =>
      throw new Exception("incomplete read command : " + r)
  }
  
  def ksDef(ks : KeyspaceDef) : KsDef = {
    val ksdef = new KsDef(ks.name, 
      ks.strategyClass, 
      ks.replicationFactor, 
      ks.cfDefs.map(cfDef(_)))
    for(options <- ks.strategyOptions) ksdef.strategy_options = options
    ksdef
  }
  
  def cfDef(cf : ColumnFamilyDef) : CfDef = {
    val cfdef = new CfDef(cf.keyspace,cf.name)
    cfdef.column_type = cf.columnType
    cfdef.clock_type = cf.clockType
    cfdef.comparator_type = cf.comparatorType
    cfdef.subcomparator_type = cf.subComparatorType
    cfdef.reconciler = cf.reconciler
    cfdef.comment = cf.comment
    cfdef.row_cache_size = cf.rowCacheSize
    cfdef.preload_row_cache = cf.preloadRowCache
    cfdef.key_cache_size = cf.keyCacheSize
    cfdef.read_repair_chance = cf.readRepairChance
    cfdef.gc_grace_seconds = cf.gcGraceSeconds
    cfdef
  }
}