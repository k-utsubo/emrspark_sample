package sample
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark._
import org.apache.spark.api.java._
import org.apache.spark.sql._
import org.apache.spark.sql.types._

//http://spark.apache.org/docs/latest/sql-programming-guide.html#upgrading-from-spark-sql-15-to-16
case class PriceHistAdj(
		stockCode:String,
		date:String,oprice:Double,high:Double,low:Double,cprice:Double,volume:Int,split:Double)
		
object SqlSample {
	def main(args: Array[String]) {
		val conf = new SparkConf().setAppName("SparkSQL").setMaster("yarn-cluster")
		val sc = new SparkContext(conf)  

		val sqlContext = new org.apache.spark.sql.SQLContext(sc)
		// Import Row.
		import org.apache.spark.sql.Row;

		// Import Spark SQL data types
		import org.apache.spark.sql.types.{StructType,StructField,StringType};

		val histRDD = sc.textFile(args(0)).map(_.split("\t")).
			map(p => Row(p(0), p(1),p(2),p(3),p(4),p(5),p(6),p(7)))
		val schemaString = "stockCode date oprice high low cprice volume split"
		val schema =
  			StructType(
    		schemaString.split(" ").map(fieldName => StructField(fieldName, StringType, true)))	
    		
		// Apply the schema to the RDD.
		val histDataFrame = sqlContext.createDataFrame(histRDD, schema)
		// Register the DataFrames as a table.
		histDataFrame.registerTempTable("priceHistAdj")
		
		// SQL statements can be run by using the sql methods provided by sqlContext.
		val results = sqlContext.sql("SELECT stockCode,date,cprice FROM priceHistAdj where stockCode='6758'")

		// The results of SQL queries are DataFrames and support all the normal RDD operations.
		// The columns of a row in the result can be accessed by field index or by field name.
		//results.map(t => "Name: " + t(0)).collect().foreach(println)
		
		val ary=results.map(_.getValuesMap[Any](List("stockCode", "date","cprice"))).collect()

		
		val outputLocation = args(1) // s3n://bucket/
		val data=sc.makeRDD(ary)
		data.saveAsTextFile(outputLocation)

		sc.stop()
	}
}