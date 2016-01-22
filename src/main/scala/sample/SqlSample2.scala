package sample
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.hive._

//http://spark.apache.org/docs/latest/sql-programming-guide.html#upgrading-from-spark-sql-15-to-16
//http://www.ne.jp/asahi/hishidama/home/tech/scala/spark/sql.html
//https://software.intel.com/en-us/blogs/2015/05/01/restudy-schemardd-in-sparksql
object SqlSample2 {
	def main(args: Array[String]) {
		val conf = new SparkConf().setAppName("SparkPi").setMaster("yarn-cluster")
		val sc = new SparkContext(conf)  
		val sqlContext = new org.apache.spark.sql.SQLContext(sc)
		//import sqlContext._

		import sqlContext.implicits._

		println(args(0))
//		val hist = sc.textFile(args(0)).map(_.split(","))
//			.map(p => PriceHistAdj(p(0), p(1),p(2).toDouble,p(3).toDouble,p(4).toDouble,p(5).toDouble,p(6).toInt,p(7).toDouble))
		
		
// Create an RDD
val people = sc.textFile(args(0))

// The schema is encoded in a string
val schemaString = "stockCode date oprice high low cprice volume split"

// Import Row.
import org.apache.spark.sql.Row;

// Import Spark SQL data types
import org.apache.spark.sql.types.{StructType,StructField,StringType};

// Generate the schema based on the string of schema
val schema =
  StructType(
    schemaString.split(" ").map(fieldName => StructField(fieldName, StringType, true)))

// Convert records of the RDD (people) to Rows.
val rowRDD = people.map(_.split(",")).map(p => Row(p(0), p(1) ,p(2),p(3),p(4),p(5),p(6),p(7)))

// Apply the schema to the RDD.
val peopleDataFrame = sqlContext.createDataFrame(rowRDD, schema)

// Register the DataFrames as a table.
peopleDataFrame.registerTempTable("priceHistAdj")

// SQL statements can be run by using the sql methods provided by sqlContext.
val results = sqlContext.sql("SELECT cprice FROM priceHistAdj where stockCode='6758'")


//=> kokokara
println(results)

		val outputLocation = args(1) // s3n://bucket/
		val data=sc.makeRDD(Seq(results))
		data.saveAsTextFile(outputLocation)
/*
// The results of SQL queries are DataFrames and support all the normal RDD operations.
// The columns of a row in the result can be accessed by field index or by field name.
results.map(t => "Date: " + t(0)).collect().foreach(println)

*/
		
		
		/*
		val hist = sc.textFile(args(0)).map(_.split(",")).
			map(p => PriceHistAdj(p(0), p(1),p(2).toDouble,p(3).toDouble,p(4).toDouble,p(5).toDouble,p(6).toInt,p(7).toDouble)).toDF()
		
		hist.registerTempTable("priceHistAdj")

		// SQL statements can be run by using the sql methods provided by sqlContext.
		val prices = sqlContext.sql("SELECT date,cprice,volume  FROM priceHistAdj WHERE stockCode='6758'")

		println(prices)
		
		// The results of SQL queries are DataFrames and support all the normal RDD operations.
		// The columns of a row in the result can be accessed by field index:
		prices.map(t => "Date: " + t(0)).collect().foreach(println)

		// or by field name:
		prices.map(t => "Cprice: " + t.getAs[String]("date")).collect().foreach(println)

		// row.getValuesMap[T] retrieves multiple columns at once into a Map[String, T]
		prices.map(_.getValuesMap[Any](List("date", "cprice"))).collect().foreach(println)
		// Map("name" -> "Justin", "age" -> 19)
		
		val outputLocation = args(1) // s3n://bucket/
		val data=sc.makeRDD(Seq(prices))
		data.saveAsTextFile(outputLocation)

		*/
		sc.stop()
	}
}