package base

import org.apache.spark.sql.SparkSession

/**
 * CSV ingestion in a dataframe.
 * Parquet Save.
 *
 * @author web2ajax@gmail.com
 */
object ViewCsv {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder
      .appName("CSV to Dataset")
      .master("local[*]")
      .getOrCreate

    val dfvendor = spark.read.format("csv")
      .option("header", "true")
      .load("src\\main\\resources\\data\\s3\\data-vendor_lookup-csv.csv")
      dfvendor.show(30)

    val dfpayment = spark.read.format("csv")
      .option("header", "true")
      .load("src\\main\\resources\\data\\s3\\data-payment_lookup-csv.csv")
      dfpayment.show(30)

    val df2009 = spark.read.format("json")
      .option("header", "true")
      .load("src\\main\\resources\\data\\s3\\data-sample_data-nyctaxi-trips-2009-json_corrigido.json")
      df2009.show(10)

    val df2010 = spark.read.format("json")
      .option("header", "true")
      .load("src\\main\\resources\\data\\s3\\data-sample_data-nyctaxi-trips-2010-json_corrigido.json")
      df2010.show(10)

    val df2011 = spark.read.format("json")
      .option("header", "true")
      .load("src\\main\\resources\\data\\s3\\data-sample_data-nyctaxi-trips-2011-json_corrigido.json")
      df2011.show(10)

    val df2012 = spark.read.format("json")
      .option("header", "true")
      .load("src\\main\\resources\\data\\s3\\data-sample_data-nyctaxi-trips-2012-json_corrigido.json")
      df2012.show(10)
    spark.stop
  }
}

