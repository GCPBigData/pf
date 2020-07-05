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
object ConverteTA_PRECOS_MEDICAMENTOS extends Serializable {

  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {

    val ss = SparkSession.builder
      .appName("CSV to TA_PRECOS_MEDICAMENTOS")
      .master("local[*]")
      .getOrCreate

    //Abri o arquivo CSV
    val TA_PRECOS_MEDICAMENTOS = ss.read
      .format("csv")
      .option("header", "true")
      .option("sep", ";")
      .option("encoding", "windows-1252")
      .option("inferSchema","True")
      .option("path","D:\\data\\TA_ORCAMENTO.csv")
      .load()

    // Converte TA_PRECOS_MEDICAMENTOS.csv para TA_PRECOS_MEDICAMENTOS.parquet
    TA_PRECOS_MEDICAMENTOS.write
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .option("encoding", "UTF-8")
      .option("path", "D:\\data\\TA_PRECOS_MEDICAMENTOS\\")
      .save()

    TA_PRECOS_MEDICAMENTOS.show(1)

    logger.info("===========Finished=========")
    ss.stop()
  }
}
