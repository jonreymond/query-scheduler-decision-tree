package imdb

import com.github.nscala_time.time.Imports._

import org.apache.spark
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

//import org.apache.spark.sql.SparkSession
//import org.apache.log4j.Logger
//import org.apache.log4j.Level
//import lsh._
import com.github.tototoshi.csv._

import java.io.File


object Main {



  def main(args: Array[String]) {



    val date = new DateTime()
    println(date.toString())
    val day = date.day.get()+ "." + date.month.get() + "."+ date.year.get()
    val time = "_" + date.hour.get() + "h" +date.minute.get()



//    val conf = new SparkConf().setAppName("app").setMaster("local[*]")
//    val sc = SparkContext.getOrCreate(conf)
//

//    sc.stop()


    val num_core_l = List(1, 2, 4, 8, 16)
    val numPartitions_l = List(1,2,4,8,16,32,64)
//    val num_core_l = List(8)
//    val numPartitions_l = List(8)
    val num_measurements = 1
    val q_list = List(
//      "q1",
//      "q2",
//      "q3",
//      "q4",
//      "q5"
      "q6"
      )


    val writers = q_list.map(x =>
                x -> CSVWriter.open(new File(STORE_PATH + x + "_" + day + ".csv"),
                                    append=false)).toMap

    writers.foreach(_._2.writeRow(List("num_cores") ++ numPartitions_l))


    num_core_l.foreach { num_core =>

      val conf = new SparkConf().setAppName("app").setMaster("local[" + num_core.toString + "]")
      val sc = SparkContext.getOrCreate(conf)

      val rdd_list = NAMES.map(load(sc,_, num_core))//.map(_.persist())
//      rdd_list.foreach(x => println(x.count()))

      q_list.foreach{q =>

        val results = numPartitions_l.map{numPartitions =>

          rdd_list.foreach(_.repartition(numPartitions))
          val queryHandler = new QueryHandler(rdd_list)

          val measurements = (1 to num_measurements).map(_ => timingInMs(queryHandler.get(q)))
          val result = measurements(0)._1
          println(result)
          val avg_timing = measurements.map(t => t._2).sum / num_measurements

          println(q + "  num_core : " + num_core + " num_partitions : " + numPartitions + " ")
          Thread.sleep(5000)
          avg_timing
        }
        writers(q).writeRow(List(num_core) ++ results)
      }
      sc.stop()
    }
    writers.foreach(_._2.close())


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
