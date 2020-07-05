package base
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, to_timestamp,date_format}

/**
 *
 * @author web2ajax@gmail.com
 */
object DayWeekAndWeekMonth extends App {

  val spark: SparkSession = SparkSession.builder()
    .master("local")
    .appName("Spark")
    .getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")

  import spark.sqlContext.implicits._

  val df = Seq(("2019-01-01 12:01:19.000","2019-01-01 14:02:19.000"),
               ("2019-02-02 12:02:19.000","2019-02-02 15:02:19.000"),
               ("2019-03-03 16:03:55.406","2019-03-03 16:02:19.000"),
               ("2019-04-04 16:04:59.406","2019-04-04 17:02:19.000")).toDF("DATA1","DATA2")

  df.withColumn("input_timestamp1", to_timestamp(col("input_timestamp1")))
    .withColumn("week_day_number", date_format(col("input_timestamp1"), "u"))
    .withColumn("week_day_abb", date_format(col("input_timestamp1"), "E"))
    .show(false)

  df.withColumn("input_timestamp1", to_timestamp(col("input_timestamp1")))
    .withColumn("week_day_full", date_format(col("input_timestamp1"), "EEEE"))
    .withColumn("week_of_month", date_format(col("input_timestamp1"), "W"))
    .show(false)

  df.withColumn("ANO_D1", date_format(col("DATA1"), "u"))
    .withColumn("DIA_SEMANA_D1", date_format(col("DATA1"), "E"))
    .withColumn("DIA_SEMANA_D2", date_format(col("DATA2"), "EEEE"))
    .withColumn("week_day_abb2", date_format(col("DATA2"), "w"))
    .show(false)

}