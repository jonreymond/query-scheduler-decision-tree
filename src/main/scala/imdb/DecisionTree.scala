package imdb

import scala.math.log10


class DecisionTree(val queries : List[String]) {
    private val queryOrder = queries
    private val probas = Map(
      "q1" -> 0.1,
      "q2" -> 0.2,
      "q3" -> 0.3,
      "q4" -> 0.1,
      "q5" -> 0.9,
      "q6" -> 0.5,
      "q7" -> 0.01,
      "q8" -> 0.76)


  // assume that length of queries is a power of 2 -1
    def getProbas(): List[Double] = {
      var results = List[Double]()
      results = results :+ 1.0
      val len = queryOrder.length
      val height = (log10(len)/log10(2.0)).toInt
      var i = 0
      while(i < height){
        var j = 0
        while(j < math.pow(2, i)){
          val idx = (j + math.pow(2, i) - 1).toInt
          val parentProbaNode = results(idx)
          val parentProba = probas(queryOrder(idx))
          results = results :+ (parentProbaNode * parentProba)
          results = results :+ (parentProbaNode * (1 -parentProba))
          j += 1
        }
        i += 1
      }

      results
    }
}
