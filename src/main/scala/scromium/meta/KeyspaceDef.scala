package scromium.meta

case class KeyspaceDef(val name : String,
  strategyClass : String,
  strategyOptions : Option[Map[String,String]],
  replicationFactor : Int,
  cfDefs : List[ColumnFamilyDef])
