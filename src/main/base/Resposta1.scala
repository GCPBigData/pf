package base

import org.apache.spark.sql.{SaveMode, SparkSession}

/**
 * 1 . Qual a distância média percorrida por viagens com no máximo 2 passageiros.
 *
 * @author web2ajax@gmail.com
 */
object Resposta1 {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder
      .appName("Parquet to Dataset")
      .master("local[*]")
      .getOrCreate

    val df2009 = spark.read.format("parquet")
      .option("header", "true")
      .load("src\\main\\resources\\data\\s3\\data-sample_data-nyctaxi-trips-2009-json_corrigido.parquet\\*.parquet")
      .drop("rate_code").drop("store_and_fwd_flag").drop("surcharge")
      .createOrReplaceTempView("ViewDf2009")

    val df2010 = spark.read.format("parquet")
      .option("header", "true")
      .load("src\\main\\resources\\data\\s3\\data-sample_data-nyctaxi-trips-2010-json_corrigido.parquet\\*.parquet")
      .drop("rate_code").drop("store_and_fwd_flag").drop("surcharge")
      .createOrReplaceTempView("ViewDf2010")

    val df2011 = spark.read.format("parquet")
      .option("header", "true")
      .load("src\\main\\resources\\data\\s3\\data-sample_data-nyctaxi-trips-2011-json_corrigido.parquet\\*.parquet")
      .drop("rate_code").drop("store_and_fwd_flag").drop("surcharge")
      .createOrReplaceTempView("ViewDf2011")

    val df2012 = spark.read.format("parquet")
      .option("header", "true")
      .load("src\\main\\resources\\data\\s3\\data-sample_data-nyctaxi-trips-2012-json_corrigido.parquet\\*.parquet")
      .drop("rate_code").drop("store_and_fwd_flag").drop("surcharge")
      .createOrReplaceTempView("ViewDf2012")

    //unifica todos arquivos parquet
    val dfSQLFull = spark.sql("SELECT * FROM ViewDf2009 UNION ALL " +
                                      "SELECT * FROM ViewDf2010 UNION ALL " +
                                      "SELECT * FROM ViewDf2011 UNION ALL " +
                                      "SELECT * FROM ViewDf2012 ORDER BY vendor_id")
    dfSQLFull.show()

    //Cria uma view com todos os arquivos parquet agrupados
    dfSQLFull.createOrReplaceTempView("dfSQLFull")

    val dfSQLViews = spark.sql("SELECT ROUND(AVG(trip_distance)) media_KM FROM dfSQLFull vp WHERE vp.passenger_count <= 2")

     dfSQLViews.write.mode(SaveMode.Overwrite).parquet("src\\main\\resources\\data\\s3\\resposta1.parquet")
     dfSQLViews.repartition(1).write.mode(SaveMode.Overwrite).csv("src\\main\\resources\\data\\s3\\resposta1.csv")
     dfSQLViews.show()
     spark.stop
  }
}