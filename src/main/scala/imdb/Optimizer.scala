package imdb

import optimus.optimization._
import optimus.optimization.enums.SolverLib
import optimus.optimization.model.{MPFloatVar, MPIntVar}
import breeze.interpolation._
import breeze.linalg.DenseVector
import optimus.optimization.model.MPBinaryVar
import Optimizer._
import optimus.algebra.AlgebraOps

class Optimizer(val name_queries : List[String], val runs_queries : List[List[(Int, Double)]], val num_cores : Int){
    require(!name_queries.isEmpty && !runs_queries.isEmpty)
    require(name_queries.length == runs_queries.length)
    //check if all maps have the same length
    require(runs_queries.forall(_.size == runs_queries.head.size))


    //can be seen as matrix T_ij = runtime of query i with j cores
    val T = runs_queries.map(interpolate(_, num_cores))

//
//    val T = Array.ofDim[Double](num_queries, num_cores)(Ts)


//    val optimizer = new Gurobi()
    implicit val model: MPModel = MPModel(SolverLib.oJSolver)
//    val test_val = MPBinaryVar("test")

    val num_queries = name_queries.length
    //maximum number of runs of parallel
    val num_runs = num_queries
    //where each query is mapped and with how many cores
    val X = Array.ofDim[MPBinaryVar](num_queries, num_cores, num_runs)
//    X(0)(0)(0) = test_val

    for(i <- 0 until num_queries;
        m <- 0 until num_cores;
        n <- 0 until num_runs)
        {
            X(i)(m)(n) = MPBinaryVar(i.toString ++ m.toString ++ n.toString)
        }


    val K = for(n <- 0 until num_runs) yield MPFloatVar(n.toString)

    for(i <- 0 until num_queries)
        {
            add(AlgebraOps.sum(X(i).flatten) := 1)
        }
    for(n <- 0 until num_runs)
    {
        val temp = X.transpose
//        add(:= 1)
    }




    start()

    println(s"objective: $objectiveValue")
//    println(s"x = ${x.value} y = ${y.value} z = ${z.value} t = ${t.value}")
//TODO : transform matrix into subsets + delete empty runs
    release()
    println("done")
}
private object Optimizer {
    private def interpolate(runs_query: List[(Int, Double)], num_cores : Int): List[Double] =
    {
        val (x1, y1) = runs_query.unzip
        val x_arr = x1.map(_.toDouble).toArray
        val y_arr = y1.toArray
        val x = new DenseVector(x_arr)
        val y = new DenseVector(y_arr)
        val f = LinearInterpolator(x, y)

        val res = for(i <- 0 until num_cores) yield f(i)

        res.toList
    }
}
