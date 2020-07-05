package base

import org.apache.spark.sql.SparkSession

/**
  * 3 . Faça um histograma da distribuição mensal, nos 4 anos, de corridas pagas em dinheiro; 
 * @author web2ajax@gmail.com
 */
object ReadParquetHistograma {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder
      .appName("Parquet to Dataset")
      .master("local[*]")
      .getOrCreate

    val dfvendor = spark.read.format("parquet")
      .option("header", "true")
      .load("src\\main\\resources\\data\\s3\\data-vendor_lookup.parquet\\*.parquet")
    dfvendor.createOrReplaceTempView("ViewVendor")

    val dfpayment = spark.read.format("parquet")
      .option("header", "true")
      .load("src\\main\\resources\\data\\s3\\data-payment_lookup.parquet\\*.parquet")
    dfpayment.createOrReplaceTempView("Viewpayment")

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

    //Gambiarra
    val dfSQLViews = spark.sql("SELECT " +
      "year(a.pickup_datetime) AS ano2009," +
      "year(b.pickup_datetime) AS ano2010," +
      "year(c.pickup_datetime) AS ano2011," +
      "year(d.pickup_datetime) AS ano2012," +
      "month(a.pickup_datetime) AS mes2009," +
      "month(b.pickup_datetime) AS mes2010," +
      "month(c.pickup_datetime) AS mes2011," +
      "month(d.pickup_datetime) AS mes2012," +
      "count(*) AS qtd " +
      "FROM ViewDf2009 AS a " +
      "INNER JOIN ViewDf2010 AS b ON (a.vendor_id = b.vendor_id) " +
      "INNER JOIN ViewDf2011 AS c  ON (a.vendor_id = c.vendor_id) " +
      "INNER JOIN ViewDf2012 AS d ON (a.vendor_id = d.vendor_id) " +
      "WHERE a.payment_type " +
      "LIKE 'CASH%' GROUP BY ano2009, ano2010, ano2011, ano2012,  " +
      "mes2009, mes2010, mes2011, mes2012"
    )
    //Otimizado para plotar
    val dfSQLViewsOtimizada = spark.sql("SELECT '2009' AS ano, month(pickup_datetime) AS mes, count() AS qtd " +
      "FROM ViewDf2009 " +
      "WHERE payment_type " +
      "LIKE 'CASH%' " +
      "GROUP BY ano, mes " +
      "UNION ALL SELECT '2010' AS ano, month(pickup_datetime) AS mes, count() AS qtd " +
      "FROM ViewDf2010 " +
      "WHERE payment_type " +
      "LIKE 'CASH%' " +
      "GROUP BY ano, mes UNION ALL " +
      "SELECT '2011' AS ano, month(pickup_datetime) AS mes, count() AS qtd " +
      "FROM ViewDf2011 " +
      "WHERE payment_type " +
      "LIKE 'CASH%' GROUP BY ano, mes UNION ALL " +
      "SELECT '2012' AS ano, month(pickup_datetime) AS mes, count() AS qtd " +
      "FROM ViewDf2012 " +
      "WHERE payment_type " +
      "LIKE 'CASH%' " +
      "GROUP BY ano, mes"
    )
    dfSQLViews.show()
    dfSQLViewsOtimizada.show()
    dfSQLViewsOtimizada.write.parquet("src\\main\\resources\\data\\s3\\histograma.parquet")
    spark.stop
  }
}
