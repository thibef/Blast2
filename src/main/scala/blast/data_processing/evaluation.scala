package blast.data_processing

import DataStructures.{EntityProfile, IdDuplicates}
import org.apache.spark.rdd.RDD

import scala.collection.mutable.HashSet
//this import is necessary to work with java.util objects (like hashset[idduplicate] here)
import collection.JavaConversions._


class evaluation (candidates: RDD[Tuple2[String,String]]) {

  var Recall:Double = 0.0
  var Precission :Double= 0.0
    var detected_hashvalues: HashSet[Int] = null

    def get_the_stats(): Tuple2[Double, Double] = {
      val candidate_pairs: HashSet[url_duplicates] = detected_duplicates()
      val ground_pairs: HashSet[url_duplicates] = create_urlDuplicates()
      var all_duplicates: Double = ground_pairs.size.asInstanceOf[Double]
      var all_comparisons: Double = candidate_pairs.size.asInstanceOf[Double]
      var count_true: Double = 0.0

      for (trueDuplicate <- ground_pairs)
        for (potentialDuplicate <- candidate_pairs) {
          if (trueDuplicate.is_identical(potentialDuplicate)) count_true += 1.0
        }
      // the ratio of detected duplicates to all existing duplicate |D| / |groundtruth|
      var recall: Double = count_true / all_duplicates
      //the ratio of detected dulicates to all executed comparisons. |D| / |B|
      var precission: Double = count_true / all_comparisons
      Recall = recall
      Precission = precission
      return Tuple2(recall, precission)
    }

    //this hashing is not used. but might be useful in other strategies for evaluation with regards to ground_truth file
    def cal_hashCode(url1: String, url2: String): Unit = {
      var hash = 7;
      //hash = 83 * hash + url1.substring(2,-1);
      // hash = 83 * hash + this.entityId2;
      return hash;
    }

    //create a hashset of entity pairs based on the entity URL form the ground_truth file
    def create_urlDuplicates(): HashSet[url_duplicates] = {
      val ds1 = DataStructures.DatasetReader.readDataset("/media/sf_uniassignments/BLAST/dataset1_dblp")
      val ds2 = DataStructures.DatasetReader.readDataset("/media/sf_uniassignments/BLAST/dataset2_acm")
      val groundtruth = read_GroundTruth.read_groundData("/media/sf_uniassignments/BLAST/groundtruth")
      // id1 in idduplicates always refer to the first dataset
      var ground_pairs: HashSet[url_duplicates] = HashSet[url_duplicates]()
      for (iddup: IdDuplicates <- groundtruth) {
        ground_pairs.add(new url_duplicates("DS1" + ds1(iddup.getEntityId1).getEntityUrl, "DS2" + ds2(iddup.getEntityId2).getEntityUrl))
      }
      return ground_pairs

    }

    //create a hashset of entity pairs based on the entity URL form the entities connected to each other in the blocking graph
    def detected_duplicates(): HashSet[url_duplicates] = {
      var candidate_pairs: HashSet[url_duplicates] = HashSet()
      for (candidate: Tuple2[String, String] <- candidates) {
        if (candidate._1.substring(0, 3) == candidate._2.substring(0, 3)) {}
        else candidate_pairs.add(new url_duplicates(candidate._1, candidate._2))
      }
      return candidate_pairs
    }

}
