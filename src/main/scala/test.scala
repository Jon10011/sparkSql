import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

object HelloSparkSql {
  def main(args: Array[String]): Unit = {
    //获取conf
    val conf = new SparkConf().setAppName("Jon").setMaster("local[2]")

    //    //获取sc
    val sc = new SparkContext(conf)
    //
    val rdd = sc.textFile("/Users/jon/Desktop/people.json")
    println(rdd.count())
    println(rdd.collect())
    //获取sparkSession
    //    val spark = new SparkSession(sc)
    //
//    val sqlContext = new SQLContext(sc)
//    val df = sqlContext.read.json("/Users/jon/Desktop/people.json")
    //    val df = sqlContext.read.json("/Users/jon/Desktop/学习笔记/resourse/employees.json")

        val spark = SparkSession.builder().config(conf).getOrCreate()
    //    val spark = SparkSession.builder().master("local[*]").appName("Jon").getOrCreate()
    //
    //    //生成DataFrame
        val df = spark.read.json("/Users/jon/Desktop/学习笔记/resourse/employees.json")

    //展示所有数据
    df.printSchema()
    df.show()

    //DSL
    df.select("name").show

    //SQL
    //创建临时表
    df.createTempView("people")

    //    spark.sql("select * from people").show

    //关闭
    //spark.close()
    sc.stop()
  }
}
