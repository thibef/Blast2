package blast.AttributeSchema

import org.apache.spark.rdd.RDD

class AttributeMatchInduction {
  def init(ds1 :RDD[Tuple2[String,Set[String]]], ds2: RDD[Tuple2[String,Set[String]]]) : Unit = {
    val pairs = ds1.cartesian(ds2).filter{case (a,b) => a._1 < b._1}


    println("we have "+pairs.count() +" pairs.")

  }
}
