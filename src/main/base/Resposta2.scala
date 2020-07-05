package base

import org.apache.spark.sql.{SaveMode, SparkSession}

/**
 * 2 . Quais os 3 maiores ​vendors​ em quantidade total de dinheiro arrecadado.
 *
 * @author web2ajax@gmail.com
 */
object Resposta2 {

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

    val dfSQLViews = spark.sql("SELECT View4Full.vendor_id  AS vendor_id, " +
      "payment_type AS pagamento, " +
      "pickup_datetime AS data_inicio, " +
      "dropoff_datetime AS data_fim, " +
      "View4Full.total_amount AS quantidade " +
      "FROM View4Full " +
      "WHERE payment_type LIKE '%CASH' " +
      "ORDER BY quantidade DESC LIMIT 3"
    )

    dfSQLViews.show()

    dfSQLViews.createOrReplaceTempView("ViewResult")

    val ResultFinal = spark.sql("SELECT ViewResult.vendor_id AS sigla, " +
      "ViewDfVendor.name AS nome_empresa, " +
      "ViewResult.pagamento AS tipo_pagamento, " +
      "TO_DATE(ViewResult.data_inicio) AS data_inicio, " +
      "TO_DATE(ViewResult.data_fim) AS data_fim,  " +
      "ViewResult.quantidade " +
      "FROM ViewResult " +
      "LEFT JOIN ViewDfVendor ON ViewResult.vendor_id = ViewDfVendor.vendor_id")

    ResultFinal.write.mode(SaveMode.Overwrite).parquet("src\\main\\resources\\data\\s3\\resposta2.parquet")
    ResultFinal.repartition(1).write.mode(SaveMode.Overwrite).csv("src\\main\\resources\\data\\s3\\resposta2.csv")
    ResultFinal.show()
    spark.stop
  }
}