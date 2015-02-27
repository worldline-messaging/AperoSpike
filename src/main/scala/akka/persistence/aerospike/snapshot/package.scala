package akka.persistence.aerospike

package object snapshot {
    def escapepid(persistenceId:String): String = {
      persistenceId.replaceAll("_", "")
    }
	
    def gensnptkey(persistenceId:String, seqNr: Long, timestamp: Long) :String = {
	  escapepid(persistenceId)+"_"+seqNr+"_"+timestamp	  
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
	
	def gensnptkeychild(baseKey:String, childIndex: Int) :String = {
	  baseKey+"_"+childIndex	  
	}
	
	def childsnapshot(key:String):Boolean = {
	  key.count(_ == '_') > 2
	}
}