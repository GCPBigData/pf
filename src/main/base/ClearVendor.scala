package base

import org.apache.spark.sql.SparkSession

/**
 * CSV ingestion in a dataframe.
 * Parquet Save.
 *
 * @author web2ajax@gmail.com
 */
object ClearVendor {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder
      .appName("CSV to Dataset")
      .master("local[*]")
      .getOrCreate

    val dfvendor = spark.read.format("csv")
      .option("header", "true")
      .load("src\\main\\resources\\data\\s3\\data-vendor_lookup-csv.csv")
    dfvendor.show(50)
    dfvendor.write.parquet("src\\main\\resources\\data\\s3\\data-vendor_lookup.parquet")
    spark.stop
  }
}
