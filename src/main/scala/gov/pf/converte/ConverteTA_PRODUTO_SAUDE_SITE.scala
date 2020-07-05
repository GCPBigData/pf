package gov.anvisa.converte

import org.apache.log4j.Logger
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
 *
 * Converte CSV Parquet
 * fonte de dados : https://dados.anvisa.gov.br/dados/
 * Converte Todos os CSV para parquet, criando
  *
 * @author web2ajax@gmail.com - 02/07/2020
 *
 * https://github.com/GCPBigData/Anvisa-Medicamentos
 */
object ConverteTA_PRODUTO_SAUDE_SITE extends Serializable {


  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {

    val ss = SparkSession.builder
      .appName("CSV to TA_PRODUTO_SAUDE_SITE")
      .master("local[*]")
      .getOrCreate

    //Abri o arquivo CSV
    val TA_PRODUTO_SAUDE_SITE = ss.read
      .format("csv")
      .option("header", "true")
      .option("sep", ";")
      .option("encoding", "windows-1252")
      .option("inferSchema","True")
      .option("path","D:\\data\\TA_PRODUTO_SAUDE_SITE.csv")
      .load()

    // Converte TA_PRODUTO_SAUDE_SITE.csv para TA_PRODUTO_SAUDE_SITE.parquet
    TA_PRODUTO_SAUDE_SITE.write
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .option("encoding", "UTF-8")
      .option("path", "D:\\data\\TA_PRODUTO_SAUDE_SITE\\")
      .save()

    TA_PRODUTO_SAUDE_SITE.show(1)

    logger.info("===========Finished=========")
    ss.stop()
  }
}

