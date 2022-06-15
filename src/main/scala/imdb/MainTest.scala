package imdb

import com.github.nscala_time.time.Imports._
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.reflect.ClassTag

//import org.apache.spark.sql.SparkSession
//import org.apache.log4j.Logger
//import org.apache.log4j.Level
//import lsh._
import com.github.tototoshi.csv._

import java.io.File
import sys.process._



object MainTest {

  def main(args: Array[String]) {
    val numPartitions = 16

    val stream = List(
      "q3", "q4", "q5", "q6",
      "q3", "q4", "q5", "q6",
      "q3", "q4", "q5", "q6",
      "q3", "q4", "q5", "q6")


    Runner.process("q7", 16, 1)
    println("achieved")
//    val stream_test = List("q7")
//    val stream_runs = stream_test.map(Runner.load_runtime(_, numPartitions))
//    assert(stream_runs.forall(_._2 == stream_runs(0)._2))


//    val d = new DecisionTree(stream)
//    //random proba
//    val probas = d.getProbas()
//
//    var command = "python src/main/python/schedule_optimizer.py"
//    command = command + " --queries " + toPythonListString(stream)
//    command = command + " --num_partitions 16"
//    command = command + " --probas " + toPythonListString(probas)
//    println(command)
//    //get result of execution
//    val test2:String = command.!!
//
//    val groups = test2.split(';').toList
//    val res = groups.map(x=> x.split(';').toList)
//    println(res)



//    val num_cores = stream_runs(0)._2

//    val date = new DateTime()
//    println(date.toString())
//    val day = date.day.get()+ "." + date.month.get() + "."+ date.year.get()
//    val time = "_" + date.hour.get() + "h" +date.minute.get()




//    val conf = new SparkConf().setAppName("app").setMaster("local[*]")
//    val sc = SparkContext.getOrCreate(conf)
//

//    sc.stop()


//    val num_core_l = List(2,4,8,16,32,64,128,256,512,1024)
//    val num_core_l = List(4)
//    val numPartitions_l = List(8)
//    val num_measurements = 1
//    val q_list = List(
//      "q1"//,
////      "q2",
////      "q3",
////      "q4",
////      "q5"
//      )
//    println(Runtime.getRuntime.availableProcessors())
//
//    num_core_l.foreach { num_core =>
//
//      println(num_core)
//      val conf = new SparkConf().setAppName("app").setMaster("local[" + num_core.toString + "]")
//      val sc = SparkContext.getOrCreate(conf)
//      val rdd1 = load(sc, "name", num_core).asInstanceOf[RDD[Name]]
////      val rdd2 = load(sc, "name", num_core).asInstanceOf[RDD[Name]]
//      val begin = System.nanoTime()
//      rdd1.count()
//      val end = System.nanoTime()
//      println(end - begin)
//
//      sc.stop()
//    }



//    val conf = new SparkConf().setAppName("app").setMaster("local[*]")
//    val sc = SparkContext.getOrCreate(conf)
//    println("Default parallelism: " + sc.defaultParallelism)
//    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
//
//
//    val rdd_k = load(sc, "keyword", 4).asInstanceOf[RDD[Keyword]]
////    println(rdd_k.getNumPartitions)
//    println(rdd_k.takeSample(withReplacement = false,2).toList)




  }
}
