package blast.main

import org.apache.spark.sql.SparkSession
import org.apache.jena.riot.Lang
import org.apache.spark.rdd.RDD
import org.apache.hadoop.hdfs.DFSClient
import DataStructures.{Attribute, DatasetReader, EntityProfile, IdDuplicates}
import blast.AttributeSchema.AttributeProfile
import blast.AttributeSchema.AttributeMatchInduction
import blast.Blocking.Blocker
import blast.Blocking.MetaBlocker
import blast.data_processing.read_GroundTruth

import scala.collection.JavaConverters._




object Main {

  def convertFile(spark : SparkSession, inputP:String, outputP: String): Unit = {
    val ds:RDD[EntityProfile] = spark.sparkContext.parallelize(DatasetReader.readDataset(inputP))
    ds.saveAsObjectFile(outputP)
  }
  def main(args: Array[String]) {
    //initializing spark
    val spark = SparkSession.builder
      .appName(s"Blast")
      .master("local[*]")
      .getOrCreate()

    val ds1suffix = "dblp"
    val ds2suffix = "acm"

    val ds1path = "/media/sf_uniassignments/BLAST/dataset1_"+ ds1suffix
    val ds1pathScala = ds1path.concat("_scala")
    val ds2path = "/media/sf_uniassignments/BLAST/dataset2_" + ds2suffix
    val ds2pathScala = ds2path.concat("_scala")

    // for the first run, uncomment this part to format the files into spark format
    //convertFile(spark, ds1path, ds1pathScala)
    //convertFile(spark, ds2path, ds2pathScala)

    //return
    //**read dataset with spark, should use the old method first to read the data for the first time
    val dataS1 :RDD[EntityProfile] = spark.sparkContext.objectFile(ds1pathScala)
    val dataS2 :RDD[EntityProfile] = spark.sparkContext.objectFile(ds2pathScala)

    //Creates AttributeProfile class instances which calculate information regarding attributes
    val AProfileDS1 =  new AttributeProfile(dataS1)
    val AProfileDS2 =  new AttributeProfile(dataS2)

//    val size_DS1  = dataS1.count() ; val  size_DS2 = dataS2.count()

    println("DS1 size:", AProfileDS1._size ,"\tDS2 size:", AProfileDS2._size)
    println("data loaded")

//    println("entropies DS1")
//    AProfileDS1.getAttributeEntropies.collect.foreach(println)
//    println("entropies DS2")
//    AProfileDS2.getAttributeEntropies.collect.foreach(println)

    val a = new AttributeMatchInduction()

    val clusters = a.calculate(AProfileDS1, AProfileDS2)
    println("clusters are:")
    clusters.foreach(println)


    val blocker = new Blocker()
    val blocks : RDD[Tuple2[Tuple2[String, Int], List[String]]] = blocker.block(AProfileDS1,AProfileDS2, clusters )
    blocks.take(5).foreach(println)
    println("#blocks :")
    println(blocks.count)


    val mBlocker = new MetaBlocker(spark)
    mBlocker.calculate(blocks, AProfileDS1, AProfileDS2)

    /*
    //evaluation stage
    read_GroundTruth.read_groundData("/media/sf_uniassignments/BLAST/groundtruth");
     val hash_values = read_GroundTruth.get_the_hashValues().asScala
    println("# duplicate pairs:"+ hash_values.size)

    */




  }

}

