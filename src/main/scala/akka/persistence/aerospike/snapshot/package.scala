package akka.persistence.aerospike

package object snapshot {
	def gensnptkey(persistenceId:String, seqNr: Long, timestamp: Long) :String = {
	  persistenceId.replaceAll("_", "")+"_"+seqNr+"_"+timestamp	  
	}

	def keysnptpersistenceId(key: String) :String = {
	  //println("keysnptpersistenceId key="+key+" substr="+key.substring(0,key.indexOf("_")))
	  key.substring(0,key.indexOf("_"))
	}
	
	def keysnptsequence(key: String):Long = {
	  //println("keysnptsequence key="+key+" substr="+key.substring(key.indexOf("_")+1,key.lastIndexOf("_")))
	  key.substring(key.indexOf("_")+1,key.lastIndexOf("_")).toLong
	}
	
	def keysnpttimestamp(key: String):Long = {
	  key.substring(key.lastIndexOf("_")+1).toLong
	}
}