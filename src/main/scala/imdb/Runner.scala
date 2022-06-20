package imdb

import com.github.tototoshi.csv._
import org.apache.spark.{SparkConf, SparkContext}

import java.io.File

object Runner {
  /**
   * Load the runtime results of a given query, or run it if not yet existed
   *
   * @param queryName : name of query
   * @param numPartitions : the results for the desired number of partitions
   * @param numPartitions : number of repetitions if needed to measure
   * @return : map of numCore -> runtime,
   *           number of cores
   */
  def load_runtime(queryName: String, numPartitions: Int = 64, numMeasurements : Int = 4): (List[(Int, Double)], Int) = {
    try {
      val reader = CSVReader.open(new File(STORE_PATH + queryName + ".csv"))
      reader.close()
    }
    catch {
      case _ => println(queryName + " not processed, process in progress...")
                process(queryName, numPartitions, numMeasurements)
    }
    val reader = CSVReader.open(new File(STORE_PATH + queryName + ".csv"))

    val title = reader.readNext().get
    val list_partitions = title.tail.map(toInt)
    require(list_partitions.contains(numPartitions))
    val ind_partition = list_partitions.indexOf(numPartitions) + 1

    var result: List[(Int, Double)] = List()

    var isEmpty = false
    while (!isEmpty) {
      val row = reader.readNext()
      if (row.isEmpty) {
        isEmpty = true
      } else {
        val row_val = row.get
        val numCore = row_val(0).toInt
        val time = row_val(ind_partition).toDouble
        result = result :+ numCore-> time
      }
    }
    reader.close()
    (result, result.last._1)
  }


  /**
   * Evaluate query runtime
   *
   * @param queryName       : name of query
   * @param numPartitions   : number of partitions used to measure => [1, 2, 4,..., numPartitions]
   * @param numMeasurements : number of runs
   */
  def process(queryName: String, numPart: Int = 64, numMeasurements: Int = 4): Unit = {

    val num_core_l = List(1, 2, 4, 8, 16)
    var i = 1
    var numPartitions_l : List[Int] = List()
    while(i <= numPart){
      numPartitions_l = numPartitions_l :+ i
      i *= 2
    }

    val writer = CSVWriter.open(new File(STORE_PATH + queryName + ".csv"))
    writer.writeRow(List("num_cores") ++ numPartitions_l)

    num_core_l.foreach { num_core =>

      val conf = new SparkConf().setAppName("app").setMaster("local[" + num_core.toString + "]")
      val sc = SparkContext.getOrCreate(conf)
      val rdd_list = NAMES.map(load(sc, _, num_core))

      val results = numPartitions_l.map{ numPartitions =>
        rdd_list.foreach(_.repartition(numPartitions))
        val queryHandler = new QueryHandler(rdd_list)
        queryHandler.init_table(queryName)

        val measurements = (1 to numMeasurements).map(_ => timingInMs(queryHandler.get(queryName)))
        val result = measurements(0)._1
        println(result)
        val avg_timing = measurements.map(t => t._2).sum / numMeasurements

        println(queryName + "  num_core : " + num_core + " num_partitions : " + numPartitions + " ")
        Thread.sleep(5000)
        avg_timing
    }

      writer.writeRow(List(num_core) ++ results)
      sc.stop()
    }
    writer.close()
    println(queryName + " process done")
  }
}
