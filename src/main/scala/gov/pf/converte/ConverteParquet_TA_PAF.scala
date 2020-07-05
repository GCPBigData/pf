package gov.anvisa.converte

import org.apache.log4j.Logger
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
 *
 * Converte CSV Parquet
 * Parquet Save.
 * fonte de dados : https://dados.anvisa.gov.br/dados/
 * Converte Todos os CSV para parquet, criando
 * Dataset separados por ano.
 *
 * @author web2ajax@gmail.com - 02/07/2020
 *
 * https://github.com/GCPBigData/Anvisa-Medicamentos
 */
object ConverteParquet_TA_PAF extends Serializable {

  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {

    val ss = SparkSession.builder
      .appName("CSV to Dataset")
      .master("local[*]")
      .getOrCreate

    val TA_PAFSchemaStruct = StructType(List(
      StructField("NU_CNPJ_EMPRESA", StringType),
      StructField("NO_FANTASIA_EMPRESA", StringType),
      StructField("NO_RAZAO_SOCIAL_EMPRESA", StringType),
      StructField("DS_TIPO_DOCTO_PROTOCOLADO", StringType),
      StructField("NU_EXPEDIENTE", IntegerType),
      StructField("DS_ASSUNTO", StringType),
      StructField("DT_ENTRADA", StringType) //ler na doc como usar date time
    ))

    val TA_PAFSchemaDDL = "NU_CNPJ_EMPRESA, NO_FANTASIA_EMPRESA, " +
                          "NO_RAZAO_SOCIAL_EMPRESA, DS_TIPO_DOCTO_PROTOCOLADO, " +
                          "NU_EXPEDIENTE, DS_ASSUNTO, DT_ENTRADA"

   //Abri o arquivo CSV
    val TA_PAF_DF = ss.read
      .format("csv")
      .option("header", "true")
      .option("sep", ";")
      .option("encoding", "windows-1252")
      //.option("inferSchema","True")
      .option("path","D:\\data\\TA_PAF.csv")
      //.option("dateFormat","d/M/y")
      .schema(TA_PAFSchemaStruct)
      .load()

    TA_PAF_DF.show(5)

    // Cria 10.000 partições
    // Converte TA_PAF.csv para TA_PAF.parquet
    TA_PAF_DF.write
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .option("encoding", "windows-1252")
      .option("path", "D:\\data\\TA_PAF")
      //.partitionBy( "NU_CNPJ_EMPRESA")
      //.option("maxRecordsPerFile", 10000)
      .save()

    logger.info("===========Finished=========")
    ss.stop()
  }
}
