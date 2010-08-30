package scromium

import scromium.client._
import scromium.meta._

class Admin(provider : ClientProvider) {
  
  def apply[T](f : Admin => T) : T = f(this)
  def keyspace(name : String) = new KeyspaceBuilder(name)
  
  def columnFamily(keyspace : String, name : String) = 
    new ColumnFamilyBuilder(keyspace, name)
  
  def superColumnFamily(keyspace : String, name : String) = 
    new SuperColumnFamilyBuilder(keyspace, name)
    
  def create(keyspace : KeyspaceBuilder) {
    provider.withClient(_.createKeyspace(keyspace.toDefinition))
  }
  
  def create(cf : ColumnFamilyBuilder) {
    provider.withClient(_.createColumnFamily(cf.toDefinition))
  }
  
  def create(cf : SuperColumnFamilyBuilder) {
    provider.withClient(_.createColumnFamily(cf.toDefinition))
  }
  
  def dropKeyspace(name : String) {
    provider.withClient { client =>
      val spaces = client.listKeyspaces
      if (spaces.contains(name)) { client.dropKeyspace(name) }
    }
  }
  
  def renameKeyspace(from : String, to : String) {
    provider.withClient(_.renameKeyspace(from, to))
  }
  
  def dropColumnFamily(name : String) {
    provider.withClient(_.dropColumnFamily(name))
  }
  
  def renameColumnFamily(from : String, to : String) {
    provider.withClient(_.renameColumnFamily(from, to))
  }
  
  def keyspaces : Set[String] = {
    provider.withClient(_.listKeyspaces)
  }
}