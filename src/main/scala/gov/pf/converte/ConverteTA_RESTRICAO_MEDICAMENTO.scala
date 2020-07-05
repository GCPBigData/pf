package gov.anvisa.converte

import org.apache.log4j.Logger
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
 *
 * Converte CSV Parquet
 * Parquet Save.
 * fonte de dados : https://dados.anvisa.gov.br/dados/
 *
 * @author web2ajax@gmail.com - 02/07/2020
 *
 * https://github.com/GCPBigData/Anvisa-Medicamentos
 */
object ConverteTA_RESTRICAO_MEDICAMENTO extends Serializable {

  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {

    val ss = SparkSession.builder
      .appName("CSV to Dataset")
      .master("local[*]")
      .getOrCreate

    //Abri o arquivo CSV
    val TA_RESTRICAO_MEDICAMENTO = ss.read
      .format("csv")
      .option("header", "true")
      .option("sep", ";")
      .option("encoding", "windows-1252")
      .option("inferSchema","True")
      .option("path","D:\\data\\TA_RESTRICAO_MEDICAMENTO.csv")
      .load()

    // Converte TA_RESTRICAO_MEDICAMENTO.csv para TA_RESTRICAO_MEDICAMENTO.parquet
    TA_RESTRICAO_MEDICAMENTO.write
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .option("encoding", "UTF-8")
      .option("path", "D:\\data\\TA_RESTRICAO_MEDICAMENTO\\")
      .partitionBy( "DS_CATEGORIA_PRODUTO")
      .option("maxRecordsPerFile", 10000)
      .save()

    TA_RESTRICAO_MEDICAMENTO.show(1)

    logger.info("===========Finished=========")
    ss.stop()
  }
}
