package gov.anvisa.ler

import gov.anvisa.ler.LerTA_ORCAMENTO.getClass
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession

object LerTA_PAF extends  Serializable {

  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {

    val ss = SparkSession.builder
      .appName("Ler TA_PAF")
      .master("local[*]")
      .getOrCreate

    //Abri o arquivo parquet
    val TA_PAF = ss.read
      .format("parquet")
      .option("header", "true")
      .option("sep", ";")
      .option("encoding", "UTF-8")
      .option("inferSchema","True")
      .option("path","D:\\data\\TA_PAF\\*.parquet")
      .load()

    TA_PAF.show()

    logger.info("===========Finished=========")
    ss.stop()

  }
}
