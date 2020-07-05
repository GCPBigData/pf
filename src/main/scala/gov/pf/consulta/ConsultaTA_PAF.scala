package gov.anvisa.consulta

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession

/**
 *
 * Consulta Parquet
 * Parquet Save.
 * fonte de dados : https://dados.anvisa.gov.br/dados/
 *
 * @author web2ajax@gmail.com - 04/07/2020
 *
 * https://github.com/GCPBigData/Anvisa-Medicamentos
 */
object ConsultaTA_PAF extends Serializable {

  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {
    val ss = SparkSession.builder
      .appName("Consulta TA_PAF")
      .master("local[*]")
      .getOrCreate

    // Ler o parquet e cria uma view temporaria.
    val TA_PAF = ss.read
      .format("parquet")
      . option("header", "true")
      .option("inferSchema", "true")
      .option("sep", ";")
      .option("encoding", "windows-1252") //testar UTF-8
      .load("D:\\data\\TA_PAF\\*.parquet")
      .createOrReplaceTempView("View_TA_PAF_DF")

    // Ler a View com SQL
    val TA_PAF_RESULT = ss.sql("SELECT * FROM View_TA_PAF_DF").show(10)


    logger.info("===========Finished=========")
    ss.stop()

  }
}
