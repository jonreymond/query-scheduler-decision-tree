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

    val stream = List("q1", "q2", "q3", "q4", "q5", "q6", "q7", "q8")



    stream.map(Runner.process(_, 16, 1))
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






  }
}
