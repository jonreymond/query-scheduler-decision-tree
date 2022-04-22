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
    val num_measurements = 5
    val q_list = List(
      "q1",
      "q2",
      "q3",
      "q4",
      "q5"
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
          Thread.sleep(10000)
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


//  def main2(args: Array[String]) {
//    val conf = new SparkConf().setAppName("app").setMaster("local[*]")
//    val sc = SparkContext.getOrCreate(conf)
//    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
//
//    val corpus_file_name_l = List(
//      "1",
//      "10"
//      , "20")
//    val query_file_name_l = List(
//      "2",
//      "10")
//    val skew_name_l = List("", "-skew")
//
//
//    var time_l = Map[Int, List[Double]](
//      0 -> List(),
//      1 -> List(), 2 -> List())
//
//    var i = 0
//    var j = 0
//    var k = 0
//    var title = ""
//    for(c <- corpus_file_name_l){
//      val corpus_file = new File(getClass.getResource("/corpus-" + c + ".csv/part-00000").getFile).getPath
//      val rdd_corpus = to_rdd(sc, corpus_file, 1)
//      print(rdd_corpus.sample(false,0.01).collectAsMap())
//      val construction_l = Map(
//        1 -> new BaseConstructionBroadcast(sqlContext, rdd_corpus, 2),
//        2 -> new BaseConstructionBroadcast(sqlContext, rdd_corpus, 3))
//
//      for(sk <- skew_name_l){
//        for(q <- query_file_name_l){
//
//          title = title + "c" + c + "_q" + q + sk + ","
//          val query_file = new File(getClass.getResource("/queries-"+c+"-"+q+sk+".csv/part-00000").getFile).getPath
//          val rdd_query = to_rdd(sc, query_file, 0.05)
//          val t1 = System.nanoTime
////          val res_exact = exact_nn.eval(rdd_query).persist()
////          res_exact.count()
//          val duration1 = (System.nanoTime - t1) / 1e9d
//          time_l = time_l + (0 -> (time_l(0) :+ duration1))
//
//          for(c_id <- 1 to 2){
//            val t = System.nanoTime
//            val res = construction_l(c_id).eval(rdd_query)
//            res.count()
//            val duration = (System.nanoTime - t) / 1e9d
//            time_l = time_l + (c_id -> (time_l(c_id) :+ duration))
//          }
//          i += 1
//          println(i + " skew over 2")
//          print_results(time_l, title)
//        }
//        i = 0
//        j += 1
//        println(j + " query over 2")
//      }
//      i = 0
//      j = 0
//      k += 1
//      println(k + " corpus over 2")
//    }
//  }
}
