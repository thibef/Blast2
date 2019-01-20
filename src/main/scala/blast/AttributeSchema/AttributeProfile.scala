package blast.AttributeSchema

import DataStructures.{Attribute, EntityProfile}
import org.apache.spark.rdd.RDD
import scala.collection.JavaConverters._

object AttributeProfile {

  def calculateProfiles(dataset : RDD[EntityProfile]) : RDD[Tuple2[String,Set[String]]] = {
    //creates RDD[Tuple2[String,Set[String]] Tuples (attribute name, Set of attribute value tokens)
    return dataset.flatMap(calculateAttrTokens).reduceByKey(_++_)
  }



  def processAttribute(attr : Attribute) :Tuple2[String,Set[String]] =  {
    //takes one attribute object and returns a tuple with attribute name and tokens of its value
    return (attr.getName(), valTransFunction(attr.getValue()))
  }

  def calculateAttrTokens(ep: EntityProfile): Seq[Tuple2[String, Set[String]]] = {
    var attr = ep.getAttributes().asScala.toList
    return attr.map(processAttribute)
  }

  def valTransFunction(str : String): Set[String] = {


    return str.replaceAll("[^A-Za-z0-9 ]", "").split(" ").toSet
  }

  def similarity(set1: Set[String], set2 : Set[String]) : Double = {
    return set1.intersect(set2).size.toFloat/set1.union(set2).size.toFloat
  }

}
