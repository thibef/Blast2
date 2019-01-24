package blast.blocking

import blast.AttributeSchema.AttributeProfile
import DataStructures.EntityProfile

import scala.collection.mutable.ListBuffer
class blocking_main(DS1:AttributeProfile,DS2: AttributeProfile) {

    private val combined_tokens : List[Tuple2[String, Set[String]]] = combine_tokens()
    def get_combined_tokens() : List[Tuple2[String, Set[String]]] = return combine_tokens()


    private def combine_tokens(): List[Tuple2[String, Set[String]]] = {
      var tokens_union = new ListBuffer[Tuple2[String, Set[String]]]
      // only if the Attributes name are the same, it would unionize their tokens
      // *just a test strategy for the current acm dataset (both datasets have the same Attributes
      // P.S "localiterator" is key here, otherwise the task would be unserializable
      for (tpl2 <- DS2.getAttributeTokens.toLocalIterator)
        for (tpl1 <- DS1.getAttributeTokens.toLocalIterator)
          if (tpl1._1 == tpl2._1) tokens_union += Tuple2(tpl1._1, tpl1._2.union(tpl2._2))

      return tokens_union.toList
    }

    private def create_blocks (tokens: List[Tuple2[String,Set[String]]]): List[Tuple2[String,Set[EntityProfile]]] = {

      return null
    }


}