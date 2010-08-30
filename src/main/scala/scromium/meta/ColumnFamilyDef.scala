package scromium.meta

case class ColumnFamilyDef(val keyspace : String,
  val name : String,
  val columnType : String,
  val clockType : String,
  val comparatorType : String,
  val subComparatorType : String,
  val reconciler : String,
  val comment : String,
  val rowCacheSize : Double,
  val preloadRowCache : Boolean,
  val keyCacheSize : Double,
  val readRepairChance : Double,
  val gcGraceSeconds : Int)
