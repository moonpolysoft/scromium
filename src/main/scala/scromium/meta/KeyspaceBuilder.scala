package scromium.meta

class KeyspaceBuilder(val name : String) {
  var strategyClass : String = "org.apache.cassandra.locator.RackUnawareStrategy"
  var replicationFactor : Int = 3
  var strategyOptions : Option[Map[String, String]] = None
  var cfBuilders : List[ColumnFamilyBuilder] = Nil
  
  def apply[T](f : KeyspaceBuilder => T) : T = f(this)
  
  def strategyClass(c : String) : this.type = {
    strategyClass = c
    this
  }
  
  def replicationFactor(i : Int) : this.type = {
    replicationFactor = i
    this
  }
  
  def strategyOptions(map : Map[String,String]) : this.type = {
    strategyOptions = Some(map)
    this
  }
  
  def columnFamily(cfName : String) : ColumnFamilyBuilder = {
    val cfBuilder = new ColumnFamilyBuilder(name, cfName)
    cfBuilders = cfBuilder :: cfBuilders
    cfBuilder
  }
  
  def superColumnFamily(cfName : String) : SuperColumnFamilyBuilder = {
    val cfBuilder = new SuperColumnFamilyBuilder(name, cfName)
    cfBuilders = cfBuilder :: cfBuilders
    cfBuilder
  }
  
  def toDefinition = KeyspaceDef(name, 
    strategyClass, 
    strategyOptions, 
    replicationFactor, 
    cfBuilders.map(_.toDefinition))
}