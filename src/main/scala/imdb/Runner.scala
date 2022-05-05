package imdb

import com.github.tototoshi.csv._
import org.apache.spark.{SparkConf, SparkContext}

import java.io.File

object Runner {
  /**
   * Load the runtime results of a given query, or run it if not yet existed
   *
   * @param queryName : name of query
   * @return : map of numCore -> runtime, number of cores, number of partitions
   */
  def load_runtime(queryName: String): (List[(Int, Double)], Int, Int) = {
    try {
      val reader = CSVReader.open(new File(STORE_PATH + queryName + ".csv"))
      reader.close()
    }
    catch {
      case _ => process(queryName)
    }
    val reader = CSVReader.open(new File(STORE_PATH + queryName + ".csv"))

    val title = reader.readNext().get

    val numPartitions = title(1).toInt

    var result: List[(Int, Double)] = List()

    var isEmpty = false
    while (!isEmpty) {
      val row = reader.readNext()
      if (row.isEmpty) {
        isEmpty = true
      } else {

        val row_val = row.get
        val numCore = row_val(0).toInt
        val time = row_val(1).toDouble
        result = result :+ numCore-> time
      }
    }
    reader.close()
    println(result)
    (result, result.last._1, numPartitions)
  }

  /**
   * Evaluate query runtime
   *
   * @param queryName       : name of query
   * @param numPartitions   : number of partitions used to measure
   * @param numMeasurements : number of runs
   */
  def process(queryName: String, numPartitions: Int = 16, numMeasurements: Int = 4): Unit = {

    val num_core_l = List(1, 2, 4, 8, 16)

    val writer = CSVWriter.open(new File(STORE_PATH + queryName + ".csv"))
    writer.writeRow(List("num_cores", numPartitions))


    num_core_l.foreach { num_core =>

      val conf = new SparkConf().setAppName("app").setMaster("local[" + num_core.toString + "]")
      val sc = SparkContext.getOrCreate(conf)
      val rdd_list = NAMES.map(load(sc, _, num_core)) //.map(_.persist())
      //      rdd_list.foreach(x => println(x.count()))

      rdd_list.foreach(_.repartition(numPartitions))
      val queryHandler = new QueryHandler(rdd_list)

      queryHandler.init_table(queryName)

      val measurements = (1 to numMeasurements).map(_ => timingInMs(queryHandler.get(queryName)))
      val result = measurements(0)._1
      println(result)
      val avg_timing = measurements.map(t => t._2).sum / numMeasurements

      println(queryName + "  num_core : " + num_core + " num_partitions : " + numPartitions + " ")
      Thread.sleep(10000)

      writer.writeRow(List(num_core, avg_timing))
      sc.stop()
    }
    writer.close()

  }
}
