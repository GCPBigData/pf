package gov.anvisa.consulta

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession

/**
 *
 * Consulta CNPJ Tabela Spark
 *
 * fonte de dados : https://dados.anvisa.gov.br/dados/
 *
 * @author web2ajax@gmail.com - 03/07/2020
 *
 * https://github.com/GCPBigData/Anvisa-Medicamentos
 */
object ConsultaCNPJSQL extends Serializable {

  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {

    val ss = SparkSession.builder
      .appName("Consulta CNPJ")
      .master("local[*]")
      .getOrCreate

    // Ler o parquet e cria uma view temporaria.
    val TA_PAF_DF = ss.read
      .format("paquet")
      .option("header", "true")
      .option("inferSchema", "true")
      .load("D:\\data\\TA_PAF\\parquet\\NU_CNPJ_EMPRESA=01859823000482\\part-00005-93a97b58-6183-4086-bb75-255edfc3eaba.c000.snappy.parquet")
      .createOrReplaceTempView("View_TA_PAF_DF")

    // Ler a View com SQL
    //val TA_PAF_DF_SQL = ss.sql("SELECT * FROM View_TA_PAF_DF")
    //TA_PAF_DF_SQL.show(10)

    logger.info("===========Finished=========")
    ss.stop()
  }
}
