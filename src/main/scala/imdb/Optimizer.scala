package imdb

import optimus.optimization._
import optimus.optimization.enums.SolverLib
import optimus.optimization.model.{MPFloatVar, MPIntVar}
import breeze.interpolation._
import breeze.linalg.DenseVector
import optimus.optimization.model.MPBinaryVar
import LPOptimizer._
import imdb.DPOptimizer.dp_split
import optimus.algebra.{AlgebraOps, Expression, Zero}
import scala.collection.mutable.ListBuffer

//import scala.annotation.tailrec
import scala.reflect.ClassTag

//abstract class Optimizer(val name_queries : List[String], val runs_queries : List[List[(Int, Double)]], val num_cores : Int)

class LPOptimizer(val name_queries : List[String], val runs_queries : List[List[(Int, Double)]], val num_cores : Int){
    require(name_queries.nonEmpty && runs_queries.nonEmpty)
    require(name_queries.length == runs_queries.length)
    //check if all maps have the same length
    require(runs_queries.forall(_.size == runs_queries.head.size))


    //can be seen as matrix T_ij = runtime of query i with j cores
    private val T = runs_queries.map(interpolate(_, num_cores))

//
//    val T = Array.ofDim[Double](num_queries, num_cores)(Ts)


//    val optimizer = new Gurobi()
    implicit val model: MPModel = MPModel(SolverLib.oJSolver)
//    val test_val = MPBinaryVar("test")

    private val num_queries = name_queries.length
    //maximum number of runs of parallel
    private val num_runs = num_queries
    //where each query is mapped and with how many cores
    private val X = Array.ofDim[MPBinaryVar](num_queries, num_cores, num_runs)
    //to track max runtime of each runs

    private val k = for(n <- 0 until num_runs) yield MPFloatVar(n.toString)
    minimize(AlgebraOps.sum(k))

    for(i <- 0 until num_queries;
        m <- 0 until num_cores;
        n <- 0 until num_runs)
        {
            X(i)(m)(n) = MPBinaryVar(i.toString ++ m.toString ++ n.toString)
        }

    //only one assignement per query
    for(i <- 0 until num_queries)
        {
            add(AlgebraOps.sum(X(i).flatten) := 1)
        }
    //foreach runs, the assigned queries do not exceed the number of cores
    for(n <- 0 until num_runs)
    {
        var expr_n :Expression = Zero
        for(i <- 0 until num_queries;
            m <- 0 until num_cores)
            {
                expr_n += m * X(i)(m)(n)
            }
        add(expr_n <:= num_cores)
    }
    //track maximum time foreach runs
    for(n <- 0 until num_runs;
        i <- 0 until num_queries)
        {
            var expr_n :Expression = Zero
            for(m <- 0 until num_cores)
            {
                expr_n += T(i)(m) * X(i)(m)(n)
            }
            add(expr_n <:= k(n))
        }

    /**
     *
     * @return List of pairs of index of assigned bucket, name of query, number of cores needed
     */
    def compute(): (List[( Int, String, Int)], Double) = {
        start()

        println(s"objective: $objectiveValue")
        //    println(s"x = ${x.value} y = ${y.value} z = ${z.value} t = ${t.value}")
        val time = objectiveValue
        var res : List[(Int, String,  Int)] = List()
        for(i <- 0 until num_queries;
            m <- 0 until num_cores;
            n <- 0 until num_runs)
        {
            if(X(i)(m)(n).value.getOrElse(0) == 1)
                {
                    res = res :+ (n, name_queries(i), m)
                }
        }
        release()
        println("done")
        (res, time)
    }


}



private object LPOptimizer {
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
    private def transpose_shift[V:ClassTag](arr : Array[Array[Array[V]]]): Array[Array[Array[V]]] = {
        val xdim = arr.length
        val ydim = arr(0).length
        val zdim = arr(0)(0).length
        println(xdim)
        println(ydim)
        println(zdim)
        val res =  Array.ofDim[V](zdim, xdim, ydim)
        for(x <-0 until xdim;
            y <-0 until ydim;
            z <- 0 until zdim)
        {
            res(z)(x)(y) = arr(x)(y)(z)
        }
        res
    }
    private def print_array[V:ClassTag](arr : Array[Array[Array[V]]]): Unit = {
        val res = transpose_shift[V](arr)
        //    val res = arr
        for(i <-res.indices){
            res(i).foreach(row => println(row.mkString(" ")))
            println()
        }

    }
}




class DPOptimizer(val name_queries : List[String], val runs_queries : List[List[(Int, Double)]], val num_cores : Int) {
    require(name_queries.nonEmpty && runs_queries.nonEmpty)
    require(name_queries.length == runs_queries.length)
    require(runs_queries.forall(_.size == runs_queries.head.size))
    private val queries = name_queries zip runs_queries.map(DPOptimizer.interpolate(_, num_cores))


    /**
     *
     * @return List of buckets containing the names of the assigned queries
     */
    def compute(): (List[List[String]], Double) = {
        println(queries.length)
        val (total_time, splits_set) =  dp_split(queries, num_cores, 0, queries.length, Set())
        val splits = splits_set.toList.sorted :+ queries.length
        var low_bound = 0
        val buckets = ListBuffer[List[String]]()
        for(i <- splits){
            buckets += name_queries.slice(low_bound, i)
            low_bound = i
        }


        println(total_time)
        println(splits)
        println(buckets.toList)
        (buckets.toList, total_time)
}

}

private object DPOptimizer {
    /*
    sort queries increasing wrt runtime of index i
    */
    def sort_queries(name_queries : List[String], runs_queries :List[List[(Int, Double)]],
                         index : Int): (List[String], List[List[(Int, Double)]] ) = {
        val indices = runs_queries.indices
        val time_run = runs_queries.map(x => x(index)._2)
        //increase order
        val indices_sorted = (time_run zip indices).sortBy(_._1).map(_._2)
        val name_q_sorted = for (i <- indices_sorted) yield name_queries(i)
        val runs_q_sorted = for (i <- indices_sorted) yield runs_queries(i)

        (name_q_sorted, runs_q_sorted)
    }

//    /*
//    put queries into buckets of size at most num_cores
//     */
//    def put_into_buckets(name_queries : List[String], runs_queries :List[List[(Int, Double)]], num_cores : Int):
//                    Array[List[(String, List[(Int, Double)] )]] = {
//        val indices = 0 until runs_queries.length
//        var i = 0
//        var result = Array.ofDim[List[(String, List[(Int, Double)])]](math.ceil(name_queries.length *1f/ num_cores).toInt)
//        result = result.map(_ => List())
//        for(i <- indices)
//        {
//            result(i / num_cores) = result(i / num_cores) :+ (name_queries(i), runs_queries(i))
//        }
//        result
//    }

    /**
     *
     * @param queries : list of queries with runtimes
     * @param num_cores : num_cores
     * @param start : start of the studied subset of query, included [start, end[
     * @param end : end of the studied subset of query, not included [start, end[
     * @param acc : accumulator to get the split indexes
     * @return
     */
    def dp_split(queries : List[(String, Map[Int, Double])],
                 num_cores : Int,
                 start : Int,
                 end : Int,
                acc : Set[Int]): (Double, Set[Int]) = {

        if(queries.isEmpty || end - start == 0)
            {
                (Double.MaxValue, acc)
            }
            //only one query
        else if((end - start) == 1)
        {
            val q = queries(start)
            (queries(start)._2(num_cores), acc)
        }
        else
        {
            val split = math.ceil((end - start) / 2f).toInt
            val new_acc = acc + split
            val split1_res = dp_split(queries, num_cores, start, start + split, new_acc)
            val split2_res = dp_split(queries, num_cores, start + split, end, new_acc)

            val split_res_time = split1_res._1 + split2_res._1
            val split_res_acc = split1_res._2 ++ split2_res._2
            if (queries.length > num_cores) {
                (split_res_time, split_res_acc)
            }
            else {
                //TODO : check if must be a multiple of 2, here underestimate since not use all cores
                val num_cores_per_query = num_cores / queries.length
                //TODO : check if replace by function or not

                val sub_queries = for (i <- start until end) yield queries(i)
                val max_time = sub_queries.map(_._2(num_cores_per_query)).max
                if (max_time < split_res_time) {
                    //better not to split
                    (max_time, acc)
                }
                else {
                    //better to split
                    (split_res_time, split_res_acc)
                }
            }
        }
    }

    /**
     * Returns an interpolated version of the runs of the queries (foreach number of cores, get value)
     * @param runs_query
     * @param num_cores
     * @return
     */
    private def interpolate(runs_query: List[(Int, Double)], num_cores : Int): Map[Int, Double] =
    {
        val (x1, y1) = runs_query.unzip
        val x_arr = x1.map(_.toDouble).toArray
        val y_arr = y1.toArray
        val x = new DenseVector(x_arr)
        val y = new DenseVector(y_arr)
        val f = LinearInterpolator(x, y)

        val res = for(i <- 1 to num_cores) yield i -> f(i)

        res.toMap
    }
}