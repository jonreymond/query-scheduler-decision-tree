package imdb

import optimus.optimization._
import optimus.optimization.enums.SolverLib
import optimus.optimization.model.{MPFloatVar, MPIntVar}
import breeze.interpolation._
import breeze.linalg.DenseVector

class Optimizer(val name_queries : List[String], val runs_queries : List[List[(Int, Double)]], val numCores : Int){
    require(!name_queries.isEmpty && !runs_queries.isEmpty)
    require(name_queries.length == runs_queries.length)
    //check if all maps have the same length
    require(runs_queries.forall(_.size == runs_queries.head.size))
    val (x1, y1) = runs_queries(0).unzip
    val x_list = x1.map(_.toDouble).toArray
    val y_list = y1.toArray
    val x = new DenseVector(x_list)
    val y = new DenseVector(y_list)
    val f = LinearInterpolator(x, y)
//    println(f(2))
    var i:Double = 0

    while(i < numCores){
      println(f(i))
      i+= 1
    }

}
