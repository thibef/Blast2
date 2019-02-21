package blast.data_processing

import org.apache.spark.rdd.RDD
import scala.collection.mutable.HashSet




object evaluation {
  var detected_hashvalues : HashSet[Int] = null

  def cal_hashCode(url1 : String , url2:String): Unit ={
    var hash = 7;
    //hash = 83 * hash + url1.substring(2,-1);
   // hash = 83 * hash + this.entityId2;
    return hash;
  }

  def detected_duplicates(detected: RDD[Tuple2[String,String]], groundTruth: HashSet[Integer]): Unit ={
    for( pair: Tuple2[String,String] <- detected){

    }
  }


  // the ratio of detected duplicates to all existing duplicate |D| / |groundtruth|
  def recall: Unit ={
  read_GroundTruth.get_the_hashValues()
  }

  //the ratio of detected dulicates to all executed comparisons. |D| / |B|
  def precission: Unit ={

  }
}
