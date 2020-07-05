package gov.anvisa.criaBanco

import org.apache.log4j.Logger
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
 *
 * Converte Parquet para Tabela Spark
 *
 * fonte de dados : https://dados.anvisa.gov.br/dados/
 * Converte Todos os parquet para Tabela Sqpark.
 * Dataset separados por ano.
 *
 * @author web2ajax@gmail.com - 03/07/2020
 *
 * https://github.com/GCPBigData/Anvisa-Medicamentos
 */
object CriaTabelaSpark extends Serializable {

  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {

    val ss = SparkSession.builder
      .appName("Parquet to Table")
      .master("local[3]")
      //.enableHiveSupport()
      .getOrCreate

    // Ler os arquivos parquetconvertidos
    val TA_PAF_ParquetDF = ss.read
        .format("parquet")
        .option("path", "D:\\data\\TA_PAF\\TA_PAF.parquet")
        .load()

   // DDL para criação do banco de dados ANVISA
    import ss.sql
    sql("CREATE DATABASE IF NOT EXISTS ANVISA")
    sql("USE ANVISA")

    // Cria a tabela Spark em Formato CSV
    TA_PAF_ParquetDF.write
        .mode(SaveMode.Overwrite)
        .format("csv")
        .bucketBy(5,"NU_CNPJ_EMPRESA")
        .sortBy("ANVISA.DT_ENTRADA")

    // Lista as Tabelas
    ss.catalog.listTables("ANVISA").show()

    logger.info("===========Finished=========")
    ss.stop()
  }
}
