package base

import org.apache.spark.{SparkContext, SparkConf}

object Streaming {

    def main(args: Array[String]): Unit = {

            import org.apache.spark.streaming._

            val conf = new SparkConf().setAppName("Simple Application").setMaster("local[*]")
            val sc = new SparkContext(conf)
            val hadoopConf=sc.hadoopConfiguration;

            hadoopConf.set("fs.s3.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")
            hadoopConf.set("fs.s3.awsAccessKeyId", "")
            hadoopConf.set("fs.s3.awsSecretAccessKey","")

            // Ler um JSON com o FileStream.
            val ssc = new org.apache.spark.streaming.StreamingContext(sc,Seconds(20))
            val lines = ssc.textFileStream("C:\\Users\\usuario\\Documents\\Dev\\DataSprints\\spark-streaming\\src\\main\\resources\\data\\s3\\texto.txt")
            //val lines = ssc.FileStream("src\\main\\resources\\data\\s3\\")


            val words = lines.flatMap(line => line.toLowerCase.split(" "))
            val wordCounts = words.map(word => (word, 1)).reduceByKey((a,b) => a + b)

            wordCounts.foreachRDD(rdd => {
                println("{")
                val localCollection = rdd.collect()
                println("  size:" + localCollection.length)
                localCollection.foreach(r => println("  " + r))
                println("}")
            })

            wordCounts.saveAsTextFiles("C:\\Users\\usuario\\Documents\\Dev\\DataSprints\\spark-streaming\\src\\main\\resources\\data\\s3\\")

            println("Spark Streaming")
            ssc.start()

            println("Spark: awaittermination")

            ssc.awaitTermination()
            println("Spark: done!")

}
}
