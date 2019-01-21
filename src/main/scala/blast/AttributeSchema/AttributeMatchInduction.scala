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
    print("sim dict ds1:" + maxDS2.toString())
    //get candidates within alpha of maximum
    val alpha = 0.9
    val candidatesDS1 : RDD[Tuple2[Tuple2[String,String],Double]] = sims.filter{ case ((ds1at, ds2at), sim) => sim > alpha*maxDS1(ds1at)}
    val candidatesDS2 : RDD[Tuple2[Tuple2[String,String],Double]]= sims.filter{ case ((ds1at, ds2at), sim) => sim > alpha*maxDS2(ds2at)}
    val candidatesWithSim :Seq[Tuple2[Tuple2[String,String],Double]] = candidatesDS1.collect() ++ candidatesDS2.collect()
    //candidates.foreach(println)
    //

    //finding connected edges
    val attributesDS1 = maxDS1.keySet.toList
    val attributesDS2 = maxDS2.keySet.toList
    //attribute arbitrary unique cluster ids
    val idsDS1 = (1 to attributesDS1.size).toList
    val idsDS2 = (attributesDS1.size+1 to  attributesDS1.size+attributesDS2.size)
    //attribute names and ids in tuples
    var attrNameClusterIdDS1 = (attributesDS1 zip idsDS1).toMap
    var attrNameClusterIdDS2 = (attributesDS2 zip idsDS2).toMap



    for( candidate <- candidates ){
      val attrD1 = candidate._1._1
      val attrD1Id = attrNameClusterIdDS1(attrD1)
      val attrD2 = candidate._1._2
      val attrD2Id = attrNameClusterIdDS1(attrD2)

      val minID = math.min(attrD1Id, attrD2Id)


    }



  }
}
