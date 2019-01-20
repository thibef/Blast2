package blast.main

import org.apache.spark.sql.SparkSession
import org.apache.jena.riot.Lang
import org.apache.spark.rdd.RDD
import org.apache.hadoop.hdfs.DFSClient
import DataStructures.DatasetReader
import DataStructures.EntityProfile
import DataStructures.Attribute
import blast.AttributeSchema.AttributeProfile
import blast.AttributeSchema.AttributeMatchInduction

import scala.collection.JavaConverters._




object Main {
  def main(args: Array[String]) {




    //initializing spark
    val spark = SparkSession.builder
      .appName(s"Blast")
      .master("local[*]")
      .getOrCreate()
    val dataS1Raw = DatasetReader.readDataset("/media/sf_uniassignments/BLAST/dataset1_dblp")
    //
    val dataS1 : RDD[EntityProfile]= spark.sparkContext.parallelize(dataS1Raw)
    val dataS2Raw = DatasetReader.readDataset("/media/sf_uniassignments/BLAST/dataset2_acm")
    val dataS2 : RDD[EntityProfile] = spark.sparkContext.parallelize(dataS2Raw)


    val AProfileDS1 =  AttributeProfile.calculateProfiles(dataS1)
    val AProfileDS2 =  AttributeProfile.calculateProfiles(dataS2)
    /*
      for ( (attr1name,attr1set) <- AProfileDS1.collect() ) {
      for ( (attr2name,attr2set) <- AProfileDS2.collect() ) {
        println(attr1name +" vs " + attr2name + AttributeProfile.similarity(attr1set, attr2set))
      }
    }
    */

    //AttributeMatchInduction(AProfileDS1, AProfileDS2)
    println("tokens:")
    AProfileDS1.take(10).foreach(println)

  }





}

