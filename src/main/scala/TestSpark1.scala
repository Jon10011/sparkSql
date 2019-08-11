import java.util.Properties

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

object TestSpark1 {
  def main(args: Array[String]): Unit = {

    //获取conf
    val conf = new SparkConf().setAppName("Jon").setMaster("local[2]")


    val spark = SparkSession.builder().config(conf).getOrCreate()

    val filePath = "/Users/jon/Desktop/学习笔记/test/people.json"


    val df = spark.sql("select * from kfk.test")

    val pro = new Properties()
    pro.setProperty("driver", "com.mysql.jdbc.Drriver")
    df.write.jdbc("jdbc:mysql://192.168.1.111:3306/test?user=root&password=songdong123", "spark1", pro)


  }

}
