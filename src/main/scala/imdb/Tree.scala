
package imdb
import org.apache.spark.rdd.RDD


abstract class Tree(rdds: List[RDD[Record]]) {
  def execute(): (List[Record], Double)
}


class Tree_1(rdds: List[RDD[Record]]) extends Tree(rdds) {
  def execute(): (List[Record], Double) = {
    
    (List(Undefined()), 0)
  }
}
