package base

import org.apache.spark.sql.{SaveMode, SparkSession}

/**
 * 3 . Faça um histograma da distribuição mensal, nos 4 anos, de corridas pagas em dinheiro; 
 *
 * @author web2ajax@gmail.com
 */
object Resposta3 {

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

    val dfvendor = spark.read.format("parquet")
      .option("header", "true")
      .load("src\\main\\resources\\data\\s3\\data-vendor_lookup.parquet\\*.parquet")
      .createOrReplaceTempView("ViewDfVendor")

    //unifica todos arquivos parquet
    val dfSQLFull = spark.sql("SELECT * FROM ViewDf2009 UNION ALL " +
      "SELECT * FROM ViewDf2010 UNION ALL " +
      "SELECT * FROM ViewDf2011 UNION ALL " +
      "SELECT * FROM ViewDf2012 ORDER BY vendor_id")

    //Cria uma view com todos os arquivos parquet agrupados
    dfSQLFull.createOrReplaceTempView("View4Full")

    val ResultFinal = spark.sql(
      "SELECT '2009' AS ano, month(pickup_datetime) AS mes, count(*) AS qtd FROM ViewDf2009 " +
      "WHERE payment_type LIKE 'CASH%' " +
      "GROUP BY ano, mes UNION ALL SELECT '2010' AS ano, month(pickup_datetime) AS mes, count(*) AS qtd " +
      "FROM ViewDf2010 WHERE payment_type " +
      "LIKE 'CASH%' GROUP BY ano, mes UNION ALL " +
      "SELECT '2011' AS ano, month(pickup_datetime) AS mes, count(*) AS qtd " +
      "FROM ViewDf2011 WHERE payment_type " +
      "LIKE 'CASH%' GROUP BY ano, mes UNION ALL " +
      "SELECT '2012' AS ano, month(pickup_datetime) AS mes, count(*) AS qtd " +
      "FROM ViewDf2012 WHERE payment_type " +
      "LIKE 'CASH%' GROUP BY ano, mes"
    )

    ResultFinal.write.mode(SaveMode.Overwrite).parquet("src\\main\\resources\\data\\s3\\resposta3.parquet")
    ResultFinal.write.mode(SaveMode.Overwrite).partitionBy("ano").parquet("src\\main\\resources\\data\\s3\\resposta3ano.parquet")
    ResultFinal.write.mode(SaveMode.Overwrite).partitionBy("mes").parquet("src\\main\\resources\\data\\s3\\resposta3mes.parquet")
    ResultFinal.repartition(1).write.mode(SaveMode.Overwrite).csv("src\\main\\resources\\data\\s3\\resposta3.csv")
    ResultFinal.show()
    spark.stop
  }
}