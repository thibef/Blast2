package blast.blocking

import blast.AttributeSchema.AttributeProfile
import DataStructures.EntityProfile
import DataStructures.Attribute

import scala.collection.mutable.ListBuffer
class blocking_main(DS1:AttributeProfile,DS2: AttributeProfile) {

    //private val union_of_tokens : List[Tuple2[String, Set[String]]] = cal_union_of_tokens()
    private val common_tokens : List[Tuple2[String,Set[String]]] = cal_common_tokens()

    //def get_union_tokens() : List[Tuple2[String, Set[String]]] = return union_of_tokens
    def get_common_tokens() : List[Tuple2[String, Set[String]]] = return common_tokens


    private def cal_union_of_tokens(): List[Tuple2[String, Set[String]]] = {
      var tokens_union = new ListBuffer[Tuple2[String, Set[String]]]
      // only if the Attributes name are the same, it would unionize their tokens
      // *just a test strategy for the current acm dataset (both datasets have the same Attributes
      // P.S "localiterator" is key here, otherwise the task would be unserializable
      for (tpl2 <- DS2.getAttributeTokens.toLocalIterator)
        for (tpl1 <- DS1.getAttributeTokens.toLocalIterator)
          if (tpl1._1 == tpl2._1) tokens_union += Tuple2(tpl1._1, tpl1._2.union(tpl2._2))

      return tokens_union.toList
    }

    private def cal_common_tokens() : List[Tuple2[String,Set[String]]] ={
      var common_tokens = new ListBuffer[Tuple2[String,Set[String]]]
      for (tpl2 <- DS2.getAttributeTokens.toLocalIterator)
        for (tpl1 <- DS1.getAttributeTokens.toLocalIterator)
          if (tpl1._1 == tpl2._1) common_tokens += Tuple2(tpl1._1, tpl1._2.intersect(tpl2._2))

      // refine the common tokens, by removing useless tokens
      val useless_tokens :Set[String] = Set("and","for","of","the", "a", "A")
      for (listT <- common_tokens)
        listT._2.--(useless_tokens)

      return common_tokens.toList
    }


    private def create_blocks (tokens: List[Tuple2[String,Set[String]]]): List[Tuple2[String,Set[EntityProfile]]] = {
      var block_collection = new ListBuffer[Tuple2[String,Set[String]]]
      //for( ep1 <- DS1.getEntityProfiles)
        //for(attr_ep1 <- ep1.getAttributes)




      return null
    }


}