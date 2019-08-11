
import org.apache.hadoop.fs.{FSDataOutputStream, FileSystem, Path}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object TestSpark6 {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
    conf.setAppName("TestPartition").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val hdfs: FileSystem = FileSystem.get(sc.hadoopConfiguration)

    val rdd: RDD[String] = sc.parallelize(Seq("234", "789", "123"))
    var strRdd = sc.emptyRDD[String]
    if (hdfs.exists(new Path("d:/test/test2/output.txt"))) {
      strRdd = sc.textFile("d:/test/test2/output.txt")
    }

    val outputStream: FSDataOutputStream = hdfs.create(new Path("d:/test/test2/output.txt"), true)
    val strings: Array[String] = rdd.union(strRdd).collect()

    for (line <- strings) {
      outputStream.write(line.getBytes("utf-8"))
      outputStream.write("\n".getBytes("utf-8"))
    }
    outputStream.close()
    hdfs.close()
    sc.stop()
  }
}
