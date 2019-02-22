package blast.data_processing

import DataStructures.{EntityProfile, IdDuplicates}
import org.apache.spark.rdd.RDD

import scala.collection.mutable.HashSet
//this import is necessary to work with java.util objects (like hashset[idduplicate] here)
import collection.JavaConversions._


class evaluation (candidates: RDD[Tuple2[String,String]]) {

  object evaluation {
    var detected_hashvalues: HashSet[Int] = null

    def cal_hashCode(url1: String, url2: String): Unit = {
      var hash = 7;
      //hash = 83 * hash + url1.substring(2,-1);
      // hash = 83 * hash + this.entityId2;
      return hash;
    }

    def create_urlDuplicates(): HashSet[url_duplicates] ={
      val ds1 = DataStructures.DatasetReader.readDataset("/media/sf_uniassignments/BLAST/dataset1_dblp")
      val ds2 = DataStructures.DatasetReader.readDataset("/media/sf_uniassignments/BLAST/dataset2_acm")
      val groundtruth = read_GroundTruth.read_groundData("/media/sf_uniassignments/BLAST/groundtruth")
      // id1 in idduplicates always refer to the first dataset
      var ground_pairs : HashSet[url_duplicates] = HashSet[url_duplicates]()
      for (iddup : IdDuplicates <- groundtruth){
        ground_pairs.add(new url_duplicates("DS1"+ds1(iddup.getEntityId1).getEntityUrl,"DS2"+ds2(iddup.getEntityId2).getEntityUrl))
      }
      return ground_pairs

    }

    def detected_duplicates(): Unit = {
      var candidate_pairs : HashSet[url_duplicates] = HashSet()
      for(candidate : Tuple2[String,String] <- candidates){
        if(candidate._1.substring(0,3) == candidate._2.substring(0,3)) {}
        else candidate_pairs.add(new url_duplicates(candidate._1,candidate._2))
      }
    }



    // the ratio of detected duplicates to all existing duplicate |D| / |groundtruth|
    def recall: Unit = {
      var detected = 0

    }

    //the ratio of detected dulicates to all executed comparisons. |D| / |B|
    def precission: Unit = {

    }
  }

}