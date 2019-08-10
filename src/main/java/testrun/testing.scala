package testrun
import scala.math.random
import org.apache.spark.sql.SparkSession

object testing {
  
  
  
  def main(args: Array[String]) {

    //val conf = new SparkConf().setAppName("Spark Pi")
//.setMaster("local")
//val sc = new SparkContext(conf)
//val sqlcontext = new SQLContext(conf)
val spark = SparkSession
   .builder()
   .config("spark.master", "local")
   .config("spark.sql.warehouse.dir", "file:///c:/tmp/spark-warehouse")
   .getOrCreate()

   val sc=spark.sparkContext
   val sqlcontext=spark.sqlContext
   
 /*  val rdd1 = sc.parallelize (Seq (("m",55), ("m",56), ("e",57),("e",58),("s",59),("s",54)))
val rdd2 = sc.parallelize (Seq (("m",60),("m",65),("s",61),("s",62),("h",63),("h",64)))
val joinrdd = rdd1.join(rdd2)
joinrdd.collect.foreach(f=> println(f._1,f._2))*/
   
   
 //  val baby_names = sqlcontext.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").load("D:\\Downloads\\Baby_Names__Beginning_2007.csv")
   
  // println(baby_names.schema)


val slices = if (args.length > 0) args(0).toInt else 2
val n = math.min(100000L * slices, Int.MaxValue).toInt // avoid overflow
val count = sc.parallelize(1 until n, slices).map { i =>
val x = random * 2 - 1
val y = random * 2 - 1
if (x*x + y*y < 1) 1 else 0
}.reduce(_ + _)
println("Pi is roughly " + 4.0 * count / n)





spark.stop()}
}