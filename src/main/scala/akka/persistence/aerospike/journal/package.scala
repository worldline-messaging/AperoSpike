package akka.persistence.aerospike

package object journal {
	def genmsgkey(persistenceId:String, seqNr: Long, marker: String) :String = {
	  persistenceId.replaceAll("_", "")+"_"+seqNr+"_"+marker	  
	}
	
	def keymsgpersistenceId(key: String): String = {
	  key.substring(0,key.indexOf("_"))
	}

	def keymsgmarker(key: String): String = {
	  //println("key="+key+" marker="+key.substring(key.length()-1))
	  key.substring(key.length()-1)
	}
	
	def keymsgsequence(key: String):Long = {
	  //println("key="+key+" i="+(key.indexOf("_")+1)+" j="+(key.lastIndexOf("_"))+" seq="+key.substring(key.indexOf("_")+1,key.lastIndexOf("_")));
	  key.substring(key.indexOf("_")+1,key.lastIndexOf("_")).toLong
	}	
}
