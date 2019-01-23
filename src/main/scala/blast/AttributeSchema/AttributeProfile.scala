package blast.AttributeSchema

import DataStructures.{Attribute, EntityProfile}
import org.apache.spark.rdd.RDD
import scala.collection.JavaConverters._
import math.log

class AttributeProfile(ds: RDD[EntityProfile]) {
  //contains each value in attributes. Important! list not set must show repeated elements
  private val _attribute_values: RDD[Tuple2[String, List[String]]] = ds.flatMap(AttributeProfile.calculateAttributeValues).reduceByKey(_ ++ _)
  private val _attribute_tokens: RDD[Tuple2[String, Set[String]]] = _attribute_values.map { case (key, valueList) => (key, AttributeProfile.calculateAttrTokens(valueList)) }
  private val _entropies = calculateAttrEntropies()

  println("attribute names are:" )
  _attribute_values.collect.foreach(x=> println(x._1))

  def getAttributeTokens: RDD[Tuple2[String, Set[String]]] = _attribute_tokens

  def getAttributeValues: RDD[Tuple2[String, List[String]]] = _attribute_values

  def getAttributeEntropies: RDD[Tuple2[String, Double]] = _entropies

  def calculateAttrEntropies(): RDD[Tuple2[String, Double]] = {

    //gets counts of each value ((attribute name, value), count )
    val attr_val_count = _attribute_values.flatMap(AttributeProfile.flatten_tuples).reduceByKey(_ + _)

    //gets total number of values for each attribute
    val attr_val_size = _attribute_values.map { case (key, valueList) => (key, valueList.size) }.collectAsMap()


    var plogp =  (p: Double) => if (p == 0.0) 0.0 else -p * log(p)
    //calculate entropies
    return attr_val_count.map { case ((aName, value), count) => (aName, plogp(count.toFloat / attr_val_size(aName))) }.reduceByKey(_ + _)
  }
}

object AttributeProfile {


  def flatten_tuples(tup : Tuple2[String, List[String]]) : List[Tuple2[Tuple2[String, String],Int]]= {
    return tup._2.map(x => ((tup._1, x),1))
  }





  //gets values of all attributes in entity profile
  def calculateAttributeValues(ep: EntityProfile): Seq[Tuple2[String, List[String]]] = {
    return ep.getAttributes().asScala.toList.map(x => (x.getName(), List(normalizeAttrValue(x.getValue()))))
  }

  def calculateAttrTokens(valueList: List[String]): Set[String] = {
    return valueList.flatMap(valTransFunction).toSet

  }

  /*
   def calculateProfiles(dataset: RDD[EntityProfile]): RDD[Tuple2[String, Set[String]]] = {
    //creates RDD[Tuple2[String,Set[String]] Tuples (attribute name, Set of attribute value tokens)
    return dataset.flatMap(calculateAttrTokens).reduceByKey(_ ++ _)
  }
  def processAttribute(attr : Attribute) :Tuple2[String,Set[String]] =  {
    //takes one attribute object and returns a tuple with attribute name and tokens of its value
    return (attr.getName(), valTransFunction(attr.getValue()))
  }

  def calculateAttrTokens(ep: EntityProfile): Seq[Tuple2[String, Set[String]]] = {
    var attr = ep.getAttributes().asScala.toList
    return attr.map(processAttribute)
  }
  */

  def valTransFunction(str: String): Set[String] = {
    return normalizeAttrValue(str).split(" ").toSet
  }

  def normalizeAttrValue(str: String): String = {
    return str.replaceAll("[^A-Za-z0-9 ]", "")
  }

  //jaccardian similarity metric
  def similarity(set1: Set[String], set2: Set[String]): Double = {
    return set1.intersect(set2).size.toFloat / set1.union(set2).size.toFloat
  }

}
