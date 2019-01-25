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
import blast.blocking.blocking_main
import scala.collection.JavaConverters._




object Main {
  def main(args: Array[String]) {




    //initializing spark
    val spark = SparkSession.builder
      .appName(s"Blast")
      .master("local[*]")
      .getOrCreate()

    //loads data into RDDs
    val dataS1Raw = DatasetReader.readDataset("/home/parsa/Downloads/dataset1_dblp")
    val dataS1  : RDD[EntityProfile]= spark.sparkContext.parallelize(dataS1Raw)
    val dataS2Raw = DatasetReader.readDataset("/home/parsa/Downloads/dataset2_acm")
    //dataS2Raw.foreach(x => println(x.getEntityUrl))
    val dataS2 : RDD[EntityProfile] = spark.sparkContext.parallelize(dataS2Raw)

    println("DS1 size",dataS1.count())
    //Creates AttributeProfile class instances which calculate information regarding attributes
    val AProfileDS1 =  new AttributeProfile(dataS1)
    val AProfileDS2 =  new AttributeProfile(dataS2)

    println("entropies DS1")
    AProfileDS1.getAttributeEntropies.collect.foreach(println)
    println("entropies DS2")
    AProfileDS2.getAttributeEntropies.collect.foreach(println)
    /*
      for ( (attr1name,attr1set) <- AProfileDS1.collect() ) {
      for ( (attr2name,attr2set) <- AProfileDS2.collect() ) {
        println(attr1name +" vs " + attr2name + AttributeProfile.similarity(attr1set, attr2set))
      }
    }
    */
    println("\n###### Match Induction details:")
    val a = new AttributeMatchInduction(AProfileDS1, AProfileDS2)
    //AttributeMatchInduction(AProfileDS1, AProfileDS2)
    println("\n\nblocking phase details:")
    println("tokens:")
    println("DS1 Tokens:")
    AProfileDS1.getAttributeTokens.foreach(println)
    println("DS2 Tokens:")
    AProfileDS2.getAttributeTokens.foreach(println)
    println("\nentity space tokens:")
    val blocking = new blocking_main(AProfileDS1,AProfileDS2)
    blocking.get_combined_tokens().foreach(println)

  }





}

