import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, DoubleType, FloatType, IntegerType, LongType, StructField, StructType}
import sun.jvm.hotspot.debugger.cdbg.IntType

/**
  * 自定义UDAF函数（用户聚合函数j）
  */
class CustomerAvg extends UserDefinedAggregateFunction {
  //输入的数据类型
  override def inputSchema: StructType = StructType(StructField("salary", LongType) :: Nil)

  //缓存的类型
  override def bufferSchema: StructType = {
    StructType(StructField("sum", LongType) :: StructField("count", LongType) :: Nil)
  }

  //返回值类型
  override def dataType: DataType = DoubleType

  //幂等性
  override def deterministic: Boolean = true

  //初始化
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = 0L
    buffer(1) = 0L
  }

  //更新
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    if (!input.isNullAt(0)) {
      buffer(0) = buffer.getLong(0) + input.getLong(0)
      buffer(1) = buffer.getLong(1) + 1
    }
  }

  //合并
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = buffer1.getLong(0) + buffer2.getLong(0)
    buffer1(1) = buffer1.getLong(1) + buffer2.getLong(1)
  }

  override def evaluate(buffer: Row): Any = {
    buffer.getLong(0).toDouble / buffer.getLong(1)
  }
}

object CustomerAvg {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster(("local[*]")).setAppName("Jon")

//    SparkContext().setLogLevel("ERROR")
    val spark = SparkSession.builder().config(conf).getOrCreate()

    import spark.implicits._

    spark.udf.register("MyAvg", new CustomerAvg)
    val df = spark.read.json("/Users/jon/Desktop/employees.json")

    df.createOrReplaceTempView("employees")
    spark.sql("select MyAvg(salary) as avg_salary from employees").show()


    spark.stop()


  }
}