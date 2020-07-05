package base

import org.apache.spark.sql.SparkSession

/**
 * CSV ingestion in a dataframe.
 * Parquet Save.
 *
 * @author web2ajax@gmail.com
 */
object ClearPayment {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder
      .appName("CSV to Dataset")
      .master("local[*]")
      .getOrCreate

    val dfpayment = spark.read.format("csv")
      .option("header", "true")
      .load("src\\main\\resources\\data\\s3\\data-payment_lookup-csv.csv")
    dfpayment.createOrReplaceTempView("ViewPayment")
    dfpayment.show(50)

    //ETL
    val dfpaymentClear = spark.sql("SELECT DISTINCT UPPER(payment_type) AS payment_type, " +
      "UPPER(payment_lookup) AS payment_lookup FROM ViewPayment")
    dfpaymentClear.show()
    dfpaymentClear.write.parquet("src\\main\\resources\\data\\s3\\data-payment_lookup.parquet")

  }
}
