package base

import org.apache.spark.sql.{SaveMode, SparkSession}

 /**
 * 4 . Faça um gráfico de série temporal contando a quantidade de gorjetas de cada dia,nos               
 * últimos 3 meses de 2012. 
 *
 * @author web2ajax@gmail.com
 */
object Resposta4 {

  def main(args: Array[String]): Unit = {

  val spark = SparkSession.builder
    .appName("Parquet to Dataset")
    .master("local[*]")
    .getOrCreate

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
    val dfSQLFull = spark.sql("SELECT * FROM ViewDf2012 ORDER BY vendor_id")

    //Cria uma view com todos os arquivos parquet agrupados
    dfSQLFull.createOrReplaceTempView("ViewSQLFull")

/*    val dfSQLViews = spark.sql("SELECT TO_DATE(dropoff_datetime), " +
      "TO_DATE(dropoff_datetime), tip_amount FROM ViewSQLFull ORDER BY dropoff_datetime DESC LIMIT 3")
*/
    val dfSQLViews = spark.sql("SELECT day(pickup_datetime) AS dia, count(*) AS qtd " +
      "FROM ViewSQLFull " +
      "WHERE payment_type " +
      "LIKE 'CASH%' " +
      "GROUP BY dia " +
      "ORDER BY 1 DESC " +
      "LIMIT 90"
    )

    dfSQLViews.write.mode(SaveMode.Overwrite).parquet("src\\main\\resources\\data\\s3\\resposta4.parquet")
    dfSQLViews.repartition(1).write.mode(SaveMode.Overwrite).csv("src\\main\\resources\\data\\s3\\resposta4.csv")
    dfSQLViews.show()
    spark.stop
  }
 }