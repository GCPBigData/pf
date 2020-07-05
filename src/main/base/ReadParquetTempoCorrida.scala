package base

import org.apache.spark.sql.SparkSession

/**
 * Qual o tempo médio das corridas nos dias de sábado e domingo
 *
 * Note: 1=Sunday, 2=Monday, 3=Tuesday, 4=Wednesday, 5=Thursday, 6=Friday, 7=Saturday.
 *
 * @author web2ajax@gmail.com
 */
object ReadParquetTempoCorrida {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder
      .appName("Parquet to Dataset")
      .master("local[*]")
      .getOrCreate

    val df2009 = spark.read.format("parquet")
      .option("header", "true")
      .load("src\\main\\resources\\data\\s3\\data-sample_data-nyctaxi-trips-2009-json_corrigido.parquet\\*.parquet")
    df2009.createOrReplaceTempView("ViewDf2009")

    val df2010 = spark.read.format("parquet")
      .option("header", "true")
      .load("src\\main\\resources\\data\\s3\\data-sample_data-nyctaxi-trips-2010-json_corrigido.parquet\\*.parquet")
    df2010.createOrReplaceTempView("ViewDf2010")

    val df2011 = spark.read.format("parquet")
      .option("header", "true")
      .load("src\\main\\resources\\data\\s3\\data-sample_data-nyctaxi-trips-2011-json_corrigido.parquet\\*.parquet")
    df2011.createOrReplaceTempView("ViewDf2011")

    val df2012 = spark.read.format("parquet")
      .option("header", "true")
      .load("src\\main\\resources\\data\\s3\\data-sample_data-nyctaxi-trips-2012-json_corrigido.parquet\\*.parquet")
    df2012.createOrReplaceTempView("ViewDf2012")

    //unifica todos arquivos parquet
    val dfSQLFull = spark.sql("SELECT * FROM ViewDf2009 UNION ALL " +
      "SELECT * FROM ViewDf2010 UNION ALL " +
      "SELECT * FROM ViewDf2011 UNION ALL " +
      "SELECT * FROM ViewDf2012 ORDER BY vendor_id")

    dfSQLFull.show()

    //Cria uma view com todos os arquivos parquet agrupados
    dfSQLFull.createOrReplaceTempView("dfSQLFull")

    val dfSQLViews = spark.sql("SELECT " +
      "floor(media/3600) AS hora, floor((media - floor(media/3600)*3600)%60) AS minuto " +
      "FROM " +
      "(SELECT AVG(CAST(to_timestamp(dropoff_datetime) AS bigint) - CAST(to_timestamp(pickup_datetime) AS bigint)) AS media " +
      "FROM " +
      "dfSQLFull " +
      "WHERE " +
      "dayofweek(pickup_datetime) in (1, 0) " +
      "OR dayofweek(pickup_datetime) in (7, 0) " +
      "OR dayofweek(dropoff_datetime) in (1, 0)" +
      "OR dayofweek(dropoff_datetime) in (7, 0))"
    )
    dfSQLViews.show()
    spark.stop
  }
}
