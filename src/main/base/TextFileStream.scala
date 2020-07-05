package base

import org.apache.log4j.{Level, LogManager}
import org.apache.spark.{HashPartitioner, SparkConf}
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming._

object TextFileStream {
  def main(args: Array[String]) {
    var sparkConfig = new SparkConf()
      .setMaster("local[*]")
      .setAppName("TextFileStream")

    val ssc = new StreamingContext(sparkConfig, Seconds(10))
    ssc.checkpoint("src\\main\\resources\\data\\s3\\data")
    val lines = ssc.textFileStream("src\\main\\resources\\data\\s3\\data\\texto.txt")
    LogManager.getRootLogger.setLevel(Level.WARN)
    val r = lines.map( line => (line, 1L) ).reduceByKey(_ + _).updateStateByKey(updateFunction)
    r.print()
    r.foreachRDD { (rdd : RDD[(String, Long)], time : Time) =>
      println(">>>Time: " + time + " Number of partitions: " + rdd.getNumPartitions)
      rdd.saveAsTextFile("src\\main\\resources\\data\\s3\\data\\")
    }
    ssc.start()
    ssc.awaitTermination()
  }

  def updateFunction(newData: Seq[Long], state: Option[Long]) = {
    val newState = state.getOrElse(0L) + newData.sum
    Some(newState)
  }

  def updateFunction2(iterator: Iterator[(String, Seq[Long], Option[Long])]) = {
    iterator.toIterable.map {
      case (key, values, state) => (key, state.getOrElse(0L) + values.sum)
    }.toIterator
  }
  def mappingFunction(key: String, value: Option[Long], state: State[Long]): (String, Long) = {
    // Use state.exists(), state.get(), state.update() and state.remove() to manage state
    value match {
      case Some(v) => {
        state.update(state.getOption().getOrElse(0L) + v)
        //return the (word, count)
        (key, state.get())
      }
      case _ => (key, 0L)
    }
  }
}