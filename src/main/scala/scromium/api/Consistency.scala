package scromium.api

import org.apache.cassandra.thrift.ConsistencyLevel

abstract sealed class ReadConsistency(val thrift : ConsistencyLevel) { 
}

object ReadConsistency {
  case object One extends ReadConsistency(ConsistencyLevel.ONE)
  case object Quorum extends ReadConsistency(ConsistencyLevel.QUORUM)
  case object All extends ReadConsistency(ConsistencyLevel.ALL)
}

abstract sealed class WriteConsistency(val thrift : ConsistencyLevel) {
}

object WriteConsistency {
  case object Zero extends WriteConsistency(ConsistencyLevel.ZERO)
  case object One extends WriteConsistency(ConsistencyLevel.ONE)
  case object Any extends WriteConsistency(ConsistencyLevel.ANY)
  case object Quorum extends WriteConsistency(ConsistencyLevel.QUORUM)
  case object All extends WriteConsistency(ConsistencyLevel.ALL)
}
