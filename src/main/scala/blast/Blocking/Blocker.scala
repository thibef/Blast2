package blast.Blocking

import blast.AttributeSchema.AttributeProfile
import DataStructures.{Attribute, EntityProfile}
import org.apache.spark.rdd.RDD

class Blocker {
  def block(DS1:AttributeProfile,DS2: AttributeProfile, attrClusters : Seq[Tuple3[Int, List[String], List[String]]]): RDD[Tuple2[Tuple2[String, Int], List[String]]] ={
    //map each attribute names to their respective cluster ids:
    val attrDS1toCluster : Map[String, Int] = attrClusters.flatMap{case (clusterID, attrD1, _) => attrD1.map{x => (x, clusterID)}}.toMap
    val attrDS2toCluster : Map[String, Int] = attrClusters.flatMap{case (clusterID, _, attrD2) => attrD2.map{x => (x, clusterID)}}.toMap

    //we want (token, cluster id, list[entity ids]
                                                                  //(attribute name, list(values)
    val listOfBlocks = DS1.getEntityProfiles.flatMap(entity => Blocker.blockEntity(entity,attrDS1toCluster, "DS1")) ++ DS2.getEntityProfiles.flatMap(entity => Blocker.blockEntity(entity,attrDS1toCluster, "DS2"))

    val result_before_purging = listOfBlocks.aggregateByKey(List[String]())(Blocker.addEntityIdToList, (a: List[String], b : List[String]) => a++b)
    //block purging
    val treshold = (DS1._size+DS2._size)/2
    println("treshold :",treshold)
    val result_after_purging = result_before_purging.filter(_._2.size < treshold)

    return result_after_purging
  }
}

object Blocker {

  def addEntityIdToList(l : List[String], v : String) = v :: l

  def blockEntity(ep : EntityProfile, attrToCluster: Map[String, Int], entityIdPrefix : String) : Seq[Tuple2[Tuple2[String, Int], String]] = {
    val attrTokens = AttributeProfile.calculateAttributeValues(ep).map{case (attrName, valueList) => (attrName, valueList.flatMap(AttributeProfile.valTransFunction))}

    val flatTokens = attrTokens.flatMap{case (attrName, tokenList) => tokenList.map(x => (attrName, x))}
    val entityID = entityIdPrefix + ep.getEntityUrl()
    return flatTokens.map{case (attrName, token) => ((token, attrToCluster(attrName)), entityID)}
    //return AttributeProfile.calculateAttributeValues(ep).flatMap{case (attrName, tokenList) => tokenList.map(x => (x, attrDS1toCluster(x), ep.getEntityUrl))}
  }


}