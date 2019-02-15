package blast.AttributeSchema


import org.apache.spark.rdd.RDD

import scala.collection.mutable

class AttributeMatchInduction() {
  //def this(ds1 :RDD[Tuple2[String,Set[String]]], ds2: RDD[Tuple2[String,Set[String]]]) = {
  def this(AP1 : AttributeProfile, AP2 : AttributeProfile) = {
    this()
    //assembles into pairs
    val ds1 = AP1.getAttributeTokens
    val ds2 = AP2.getAttributeTokens
    val pairs = ds1.cartesian(ds2)
    println("we have "+pairs.count() +" pairs.")
    val pairsNames  = pairs.map{case (a,b) => (a._1,b._1) }
    val pairsList : Seq[Tuple2[String,String]] = pairsNames.collect()
    println(pairsList)


    //calculates similarities
    val sims : RDD[Tuple2[Tuple2[String,String],Double]]= pairs.map{case (a,b) => ((a._1,b._1),AttributeProfile.similarity(a._2,b._2)) }

    //val simsList : Seq[Tuple2[Tuple2[String,String],Double]] = sims.collect()

    //maximum similarity wrt dataset1
    val maxDS1 = sims.map{case ((a,_),b) => (a, b)}.reduceByKey(math.max(_,_)).collectAsMap()
    print("sim dict ds1:" + maxDS1.toString())
    //maximum similarity wrt dataset2
    val maxDS2 = sims.map{case ((_,a),b) => (a, b)}.reduceByKey(math.max(_,_)).collectAsMap()
    print("sim dict ds2:" + maxDS2.toString())

    //get candidates within alpha of maximum
    val alpha = 0.9
    val candidatesDS1 : RDD[Tuple2[Tuple2[String,String],Double]] = sims.filter{ case ((ds1at, ds2at), sim) => sim > alpha*maxDS1(ds1at)}
    val candidatesDS2 : RDD[Tuple2[Tuple2[String,String],Double]]= sims.filter{ case ((ds1at, ds2at), sim) => sim > alpha*maxDS2(ds2at)}
    val candidatesWithSim :Set[Tuple2[Tuple2[String,String],Double]] = (candidatesDS1.collect() ++ candidatesDS2.collect()).toSet
    println("candidate pairs are:")
    candidatesWithSim.foreach(println)
    println("end  candidates")
    //

    //finding connected edges
    val attributesDS1 = maxDS1.keySet.toList
    val attributesDS2 = maxDS2.keySet.toList
    //attribute arbitrary unique cluster ids

    var clusterIdsDS1 = (1 to attributesDS1.size).toList
    var clusterIdsDS2 = (attributesDS1.size+1 to  attributesDS1.size+attributesDS2.size).toList
    //attribute names and indexes in tuples
    var attrNameIndexDS1 = (attributesDS1 zip (0 to attributesDS1.size-1 ).toList).toMap
    var attrNameIndexDS2 = (attributesDS2 zip (0 to attributesDS2.size-1 ).toList).toMap

    //finds connected components in graphs. assigns keys to each connected maximal subgraph.
    for( candidate <- candidatesWithSim ){
      val attrNameD1 = candidate._1._1
      val attrD1Cluster = clusterIdsDS1(attrNameIndexDS1(attrNameD1))
      val attrNameD2 = candidate._1._2
      val attrD2Cluster = clusterIdsDS2(attrNameIndexDS2(attrNameD2))

      val minCluster = math.min(attrD1Cluster, attrD2Cluster)
      //replace every instance of old ideas by minID
                    // x => if expr retval else retval
      //clusterIdsDS1 = clusterIdsDS1.map{case attrD1Cluster => minCluster; case x => x}
      clusterIdsDS1 = clusterIdsDS1.map{x => if (x== attrD1Cluster) minCluster else x}
      //clusterIdsDS2 = clusterIdsDS2.map{case attrD2Cluster => minCluster; case x => x}
      clusterIdsDS2 = clusterIdsDS2.map{x => if (x == attrD2Cluster) minCluster else x}
    }


  }
}
