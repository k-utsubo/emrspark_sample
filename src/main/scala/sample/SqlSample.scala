package sample
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark._
import org.apache.spark.api.java._

//http://spark.apache.org/docs/latest/sql-programming-guide.html#upgrading-from-spark-sql-15-to-16
case class PriceHistAdj(
		stockCode:String,
		date:String,oprice:Double,high:Double,low:Double,cprice:Double,volume:Int,split:Double)
		
object SqlSample {
	def main(args: Array[String]) {
		val conf = new SparkConf().setAppName("SparkPi").setMaster("yarn-cluster")
		val sc = new SparkContext(conf)  
		val sqlContext = new org.apache.spark.sql.SQLContext(sc)
		import sqlContext.implicits._
		
		val hist = sc.textFile(args(0)).map(_.split(",")).
			map(p => PriceHistAdj(p(0), p(1),p(2).toDouble,p(3).toDouble,p(4).toDouble,p(5).toDouble,p(6).toInt,p(7).toDouble)).toDF()
		hist.registerTempTable("priceHistAdj")

		// SQL statements can be run by using the sql methods provided by sqlContext.
		val prices = sqlContext.sql("SELECT date,cprice,volume  FROM priceHistAdj WHERE stockCode='6758'")

		// The results of SQL queries are DataFrames and support all the normal RDD operations.
		// The columns of a row in the result can be accessed by field index:
		prices.map(t => "Date: " + t(0)).collect().foreach(println)

		// or by field name:
		prices.map(t => "Cprice: " + t.getAs[String]("date")).collect().foreach(println)

		// row.getValuesMap[T] retrieves multiple columns at once into a Map[String, T]
		prices.map(_.getValuesMap[Any](List("date", "cprice"))).collect().foreach(println)
		// Map("name" -> "Justin", "age" -> 19)
		
		val outputLocation = args(0) // s3n://bucket/
		val data=sc.makeRDD(Seq(prices))
		data.saveAsTextFile(outputLocation)
		sc.stop()
	}
}