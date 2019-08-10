package dbs01

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{ StructType, StructField, StringType, IntegerType, TimestampType }
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.{ col, count, avg, sum, max, when, udf }
import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.Date
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.api.java.UDF1

object obj1 {

  def getTimestamp(x: Any): Timestamp = {
    val format = new SimpleDateFormat("MM/dd/yyyy' 'HH:mm:ss")
    if (x.toString() == "")
      return null
    else {
      val d = format.parse(x.toString());
      val t = new Timestamp(d.getTime());
      return t
    }
  }
  def main(args: Array[String]) {
    println("DBS Hacktron")
    System.setProperty("hadoop.home.dir", "C:\\Users\\DELL\\Downloads\\winutils-master\\winutils-master\\hadoop-2.6.0")
    val session = SparkSession.builder().master("local").getOrCreate()

    import session.implicits._

    val prod = session.sparkContext.textFile("H:\\BigData\\Product.txt")
    val sale = session.sparkContext.textFile("H:\\BigData\\Sales.txt")

    val schema = StructType(Seq(StructField("product_id", IntegerType, true), StructField("product_name", StringType, true), StructField("product_type", StringType, true), StructField("product_version", StringType, true), StructField("product_price", StringType, true)))

    val rdd1 = prod.map(x => x.split('|')).map(y => Row(y(0).toInt, y(1), y(2), y(3), y(4)))

    val prod_output = session.createDataFrame(rdd1, schema)

    //reading Sales table

    val schema_sales = StructType(Seq(StructField("transaction_id", IntegerType, true), StructField("customer_id", IntegerType, true), StructField("product_id", IntegerType, true), StructField("timestamp", TimestampType, true), StructField("total_amount", StringType, true), StructField("total_quantity", IntegerType, true)))
    val rdd2 = sale.map(x => x.split('|')).map(y => Row(y(0).toInt, y(1).toInt, y(2).toInt, getTimestamp(y(3)), y(4), y(5).toInt))

    val sales_output = session.createDataFrame(rdd2, schema_sales)

    //sales_output.show
    // prod_output.printSchema

    //first join two dataframes
    val joined_Sales_Product = sales_output.join(prod_output, Seq("product_id"))
    //df2.show
    // df2.show

    val checkDollar = udf[String, String](check)

    val dollarReplaced = joined_Sales_Product.withColumn("amount", when(joined_Sales_Product("total_amount").contains("$"), checkDollar(joined_Sales_Product("total_amount"))).otherwise(joined_Sales_Product("total_amount"))).drop(joined_Sales_Product("total_amount")).withColumnRenamed("amount", "total_amount")

    val q2 = dollarReplaced.groupBy("product_name", "product_type").agg(sum(dollarReplaced("total_amount")).as("total_sum"), sum(dollarReplaced("total_quantity")).as("Total_Quantity"))

    /* val w = Window.partitionBy(joined_Sales_Product("product_name"), joined_Sales_Product("product_type"))

    dollarReplaced.groupBy("product_name", "product_type").agg(avg(dollarReplaced("product_price").over(w))).show
*/

    //Q4: find a product that has not been sold at least once
    //Q4:product whose product_id is not present in sales table is not sold.

    val df4 = prod_output.join(sales_output, sales_output("product_id") === prod_output("product_id"), "left_outer")
    val df5 = df4.filter(prod_output("product_id").isNull)

  }

  def check(s: String): String = {

    s.replace('$', ' ')

  }
}