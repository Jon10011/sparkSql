import java.io.{File, OutputStreamWriter, PrintWriter}
import java.nio.charset.Charset

import org.apache.hadoop.fs.Path
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}


object TestSpark5 {

  def main(args: Array[String]): Unit = {

    val inputPath = args(0)
    val defaultFS = args(1)


    val conf = new SparkConf().set("fs.defaultFS", defaultFS);
    conf.setAppName("TestPartition")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    //    val hdfs: FileSystem = FileSystem.get(sc.hadoopConfiguration)

    //    val hadoopConf = sc.hadoopConfiguration
    //    val hdfs = org.apache.hadoop.fs.FileSystem.get(hadoopConf)


    //    val spark: SparkSession = SparkSession.builder().master("192.168.1.111:9000").appName("123").getOrCreate()

    //    val rdd = sc.textFile("D:/wordcount.txt")
    //    val result = rdd.flatMap(x => {
    //      val sc1 = new SparkContext()
    //      val arr = x.split(" ")
    //      val rdd1 = sc1.textFile("D:/wordcount.txt")
    //      arr
    //    })

    //    import sqlContext.implicits._

    val rdd: RDD[String] = sc.parallelize(Seq("234", "789", "123"))
    var strRdd = sc.emptyRDD[String]

    val inputPath1 = new Path(inputPath)
    val fs = inputPath1.getFileSystem(sc.hadoopConfiguration)

    //    val path = new Path(inputPath)
    if (!fs.exists(inputPath1)) {
    } else strRdd = sc.textFile(inputPath)
    val file1 = fs.create(inputPath1, true)

        strRdd.persist(StorageLevel.MEMORY_AND_DISK)
    val strings: Array[String] = rdd.union(strRdd).collect()

//    val writer = new PrintWriter(fs.create(inputPath1, true))
    //    val writer = new PrintWriter(new OutputStreamWriter(strRdd,Charset.forName("UTF-8")))
    for (line <- strings) {
      file1.write(line)
      file1.write("\n")
    }
    file1.close()
    //      .toDF("str")
    //      .coalesce(1).write.mode(SaveMode.Overwrite).format("text").save("d:/test/test2")

    println("来了老弟")
    //
    //    //    val rdd1: DataFrame = spark.read.format("text").text("d:/test/test1")
    //    val rdd1 = sc.textFile("hdfs://hdp-01:9000/test08101.txt")
    //    rdd1.collect().foreach(x => {
    //      println(x + "%%%%%%%")
    //    })
    //
    //    //    val result: RDD[String] = sc.textFile("d:/test/test/*")
    //    //    result.collect().foreach(println)
    //    strRdd.unpersist()
    fs.close()
    sc.stop()
  }
}
