package base

import org.apache.spark.sql.{SaveMode, SparkSession}

/**
 * CSV ingestion in a dataframe.
 * Parquet Save.
 *
 * @author web2ajax@gmail.com
 */
object CsvToDataframeToParquet {

    def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder
      .appName("CSV to Dataset")
      .master("local[*]")
      .getOrCreate

      val df2009 = spark.read.format("json")
       .option("header", "true")
       .load("src\\main\\resources\\data\\s3\\data-sample_data-nyctaxi-trips-2009-json_corrigido.json")
      df2009.write.parquet("src\\main\\resources\\data\\s3\\data-sample_data-nyctaxi-trips-2009-json_corrigido.parquet")
      df2009.show(10)

     val df2010 = spark.read.format("json")
       .option("header", "true")
       .load("src\\main\\resources\\data\\s3\\data-sample_data-nyctaxi-trips-2010-json_corrigido.json")
     df2010.write.parquet("src\\main\\resources\\data\\s3\\data-sample_data-nyctaxi-trips-2010-json_corrigido.parquet")
     df2010.show(10)

     val df2011 = spark.read.format("json")
       .option("header", "true")
       .load("src\\main\\resources\\data\\s3\\data-sample_data-nyctaxi-trips-2011-json_corrigido.json")
     df2011.write.parquet("src\\main\\resources\\data\\s3\\data-sample_data-nyctaxi-trips-2011-json_corrigido.parquet")
     df2011.show(10)

     val df2012 = spark.read.format("json")
       .option("header", "true")
       .load("src\\main\\resources\\data\\s3\\data-sample_data-nyctaxi-trips-2012-json_corrigido.json")
     df2012.write.parquet("src\\main\\resources\\data\\s3\\data-sample_data-nyctaxi-trips-2012-json_corrigido.parquet")
     df2012.show(10)

      //Partition
      df2012.write
        .format("json")
        .mode(SaveMode.Overwrite)
        .option("path", "src\\main\\resources\\data\\s3\\data-sample_data-nyctaxi-trips-2012-json_corrigido.parquet")
        .partitionBy( "OP_CARRIER", "ORIGIN")
        .option("maxRecordsPerFile", 10000)
        .save()

    spark.stop
  }
}
