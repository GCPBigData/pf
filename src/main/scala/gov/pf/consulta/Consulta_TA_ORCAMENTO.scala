package gov.anvisa.consulta

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession

object Consulta_TA_ORCAMENTO extends Serializable {

  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {
    val ss = SparkSession.builder
      .appName("Consulta CNPJ")
      .master("local[*]")
      .getOrCreate

    // Ler o parquet e cria uma view temporaria.
    val TA_PAF_DF = ss.read
      .format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load("D:\\data\\TA_PAF\\parquet\\NU_CNPJ_EMPRESA=01859823000482\\*.parquet")
      .createOrReplaceTempView("View_TA_PAF_DF")

    // Ler a View com SQL
    val TA_PAF_DF_SQL = ss.sql("SELECT * FROM View_TA_PAF_DF")
    TA_PAF_DF_SQL.show(10)

    logger.info("===========Finished=========")
    ss.stop()
  }

}
