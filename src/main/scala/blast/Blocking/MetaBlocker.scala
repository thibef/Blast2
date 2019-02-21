package blast.Blocking
import blast.AttributeSchema.AttributeProfile
import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD

class MetaBlocker(spark : SparkSession){

  private val _spark = spark
  def calculate(blocks : RDD[Tuple2[Tuple2[String, Int], List[String]]], DS1Attr : AttributeProfile, DS2Attr : AttributeProfile){
    //tuples (entity id, entity profile)
    val entityByKeys = DS1Attr.getEntityProfiles.map(x => ("DS1"+x.getEntityUrl, x)) ++ DS2Attr.getEntityProfiles.map(x=> ("DS2"+x.getEntityUrl, x))

    //generates directed edges from blocks. one edge is added in each direction
    val edges = blocks.flatMap(MetaBlocker.createEdges)

    println("generated edges")

    //calculates the maximum weighted edge departing from each edge and calculate node-theta from it
    val c = 2.0 // c embedded into aggregator function
    val thetaPerNode = edges.aggregateByKey(0.0)((mv : Double , tup : Tuple2[String, Int]) => math.max(mv/c, tup._2), (a: Double, b : Double) => math.max(a,b))

    println("calculated thetaas")

    //we collect the theta values and broadcast them avoiding many network operations
    //https://stackoverflow.com/a/17690254
    val thetasPerNodeBrd = spark.sparkContext.broadcast(thetaPerNode.collectAsMap)

    //remove those edges that have weight less than threshold according to blast strategy and those
    //where the id of the first is > of the second. Previously we included edges in both direction to facilitate
    //the computation of the threshold we dont neet it anymore
    //then, remaps to (id1, id2) removing the weight
    val filteredEdges : RDD[Tuple2[String,String]] = edges.filter{case (a,(b, weight)) =>
      val tethaH = thetasPerNodeBrd.value //gets broadcasted hash
      val newT = (tethaH.get(a).get+ tethaH.get(b).get)/2.0
      weight >= newT && a < b}.map{case (a,(b, weight)) => (a,b)}
    filteredEdges.persist()
    println("pruned graph")

    //use same algorithms as in match induction to find subgraphs

//    val pairs = filteredEdges.collect()

//
//    //get all entity ids in the pairs
//    val entities = filteredEdges.flatMap{case (a,b) => List(a,b)}.distinct().collect().toList
//
//    println("collected entitities")
//    println("num entities:")
//    println(entities.size)
//    //generate numeric index for each pair
//    var profileCluster = (0 to entities.size).toArray
//
//    //generate map between id to index profile id
//    val mapId = (entities zip profileCluster).toMap
//    println("genereated map")
//    for( pair <- pairs ){
//      val a_id = pair._1
//      val a_index = mapId.getOrElse(a_id,0) //index in profileCluster
//      val a_cluster = profileCluster(a_index)
//      val b_id = pair._2
//      val b_index = mapId.getOrElse(b_id,0)
//      val b_cluster = profileCluster(b_index)
//
//      //replace every instance losing cluster with winner's
//      if (a_id < b_id) {
//        profileCluster = profileCluster.map { x => if (x == b_cluster ) a_cluster  else x }
//      } else {
//        profileCluster = profileCluster.map { x => if (x == a_cluster ) b_cluster  else x }
//      }
//
//    }
//    println("calculated clusters")
//    profileCluster.toSet.foreach(println)


  }
}

object MetaBlocker{

  //todo: fix weight calculation
  def createEdges(block : Tuple2[Tuple2[String, Int], List[String]] ) : List[Tuple2[String,Tuple2[ String, Int]]] = {

    val pairs = block._2.flatMap(x=>block._2.map(y=> (x,y)) ).filter{case (id1, id2) => id1!=id2}

    return pairs.map{case (id1, id2) => (id1,(id2, 1) ) }

  }

}