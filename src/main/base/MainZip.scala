package base

/**
 *
 * Parquet Save.
 *
 * @author web2ajax@gmail.com
 */
class MainZip {

  trait Decompressor extends Serializable {
    import org.apache.log4j.Logger
    import org.apache.hadoop.conf.Configuration
    import org.apache.spark.input.PortableDataStream
    import org.apache.hadoop.fs.{FileSystem, Path}
    import org.apache.spark.SparkContext
    import org.apache.spark.SparkConf


    @transient lazy val sparkConf = new SparkConf().setAppName("Decompressor")
    @transient lazy val sc = new SparkContext(sparkConf)
    @transient lazy val log = Logger.getLogger("Decompressor")
    @transient lazy val conf: Configuration = new Configuration
    @transient lazy val hdfs: FileSystem = FileSystem.get(conf)

    val tamBuffer: Integer = 8 * 1024

    def unzip(arqCompactado: String, caminhoBaseSaida: String): Unit = {
      log.info("Utilizando metodo zip")

      import java.util.zip.ZipInputStream

      val buffer = new Array[Byte](tamBuffer)

      sc.binaryFiles(arqCompactado).foreach{case (nome: String, conteudo: PortableDataStream) =>
        log.info(f"Lendo arquivo compactado ${nome}...")

        val zis = new ZipInputStream(conteudo.open)

        Stream.continually(zis.getNextEntry).takeWhile(_ != null).foreach{ arqOUdir =>
          try {
            if (! arqOUdir.isDirectory()) {
              val saidaHDFS = hdfs.create(new Path(caminhoBaseSaida ++ "/" ++ arqOUdir.getName))
              Stream.continually(zis.read(buffer)).takeWhile(_ > 0).foreach{ tam =>
                saidaHDFS.write(buffer, 0, tam)
              }
              log.info(f"Arquivo ${caminhoBaseSaida}/${arqOUdir.getName} descompactado com sucesso !")
              saidaHDFS.close
              saidaHDFS.flush
            } else {
              hdfs.mkdirs(new Path(caminhoBaseSaida ++ "/" ++ arqOUdir.getName))
              log.info(f"Diretorio ${caminhoBaseSaida}/${arqOUdir.getName} criado com sucesso !")
            }
          } catch {
            case e: Exception => {
              throw new Exception(e)
            }
          }
        }
        zis.close
      }
    }

    def untar(arqCompactado: String, caminhoBaseSaida: String): Unit = {
      log.info("Utilizando metodo tar")

      import org.apache.commons.compress.archivers.tar.TarArchiveInputStream
      import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream

      val buffer = new Array[Byte](tamBuffer)

      sc.binaryFiles(arqCompactado).foreach{case (nome: String, conteudo: PortableDataStream) =>
        log.info(f"Lendo arquivo compactado ${nome}...")

        val tars = new TarArchiveInputStream(new GzipCompressorInputStream(conteudo.open))

        Stream.continually(tars.getNextTarEntry).takeWhile(_ != null).foreach{ arqOUdir =>
          try {
            if (! arqOUdir.isDirectory()) {
              val saidaHDFS = hdfs.create(new Path(caminhoBaseSaida ++ "/" ++ arqOUdir.getName))
              Stream.continually(tars.read(buffer)).takeWhile(_ > 0).foreach{ tam =>
                saidaHDFS.write(buffer, 0, tam)
              }
              log.info(f"Arquivo ${caminhoBaseSaida}/${arqOUdir.getName} descompactado com sucesso !")
              saidaHDFS.close
              saidaHDFS.flush
            } else {
              hdfs.mkdirs(new Path(caminhoBaseSaida ++ "/" ++ arqOUdir.getName))
              log.info(f"Diretorio ${caminhoBaseSaida}/${arqOUdir.getName} criado com sucesso !")
            }
          } catch {
            case e: Exception => {
              throw new Exception(e)
            }
          }
        }
        tars.close
      }
    }

    def ungz(arqCompactado: String, caminhoBaseSaida: String): Unit = {
      log.info("Utilizando metodo gz")

      import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream

      val buffer = new Array[Byte](tamBuffer)

      sc.binaryFiles(arqCompactado).foreach{case (nome: String, conteudo: PortableDataStream) =>
        log.info(f"Lendo arquivo compactado ${nome}...")

        val gis = new GzipCompressorInputStream(conteudo.open)

        try {
          val saidaHDFS = hdfs.create(new Path(caminhoBaseSaida ++ "/" ++ gis.getMetaData.getFilename))
          Stream.continually(gis.read(buffer)).takeWhile(_ > 0).foreach{ tam =>
            saidaHDFS.write(buffer, 0, tam)
          }
          log.info(f"Arquivo ${caminhoBaseSaida}/${gis.getMetaData.getFilename} descompactado com sucesso !")
          saidaHDFS.close
          saidaHDFS.flush
          gis.close
        } catch {
          case e: Exception => {
            throw new Exception(e)
          }
        }
      }
    }
  }

  object Decompress extends Decompressor {
    def main(args: Array[String]) = {
      val arq = args(0)
      val dest = args(1)

      if (! arq.split(",").map(i => i.toLowerCase.endsWith(".zip")).contains(false)) {
        unzip(arq, dest)
      } else if (! arq.split(",").map(i => i.toLowerCase.endsWith(".tar.gz")).contains(false)) {
        untar(arq, dest)
      } else if (! arq.split(",").map(i => i.toLowerCase.endsWith(".gz")).contains(false)) {
        ungz(arq, dest)
      }  else {
        throw new IllegalArgumentException("Arquivo de entrada invalido.")
      }
    }
  }
}
