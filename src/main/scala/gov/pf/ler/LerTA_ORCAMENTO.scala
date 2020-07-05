package gov.anvisa.ler

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession

object LerTA_ORCAMENTO extends Serializable {

  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {

    val ss = SparkSession.builder
      .appName("Ler TA_ORCAMENTO")
      .master("local[*]")
      .getOrCreate

    //Abri o arquivo parquet
    val TA_ORCAMENTO = ss.read
      .format("parquet")
      .option("header", "true")
      .option("sep", ";")
      .option("encoding", "UTF-8")
      .option("inferSchema","True")
      .option("path","D:\\data\\TA_ORCAMENTO\\NU_ANO=2017\\*.parquet")
      .load()

    TA_ORCAMENTO.show()

    logger.info("===========Finished=========")
    ss.stop()

}
}
