package sample
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark._
import org.apache.spark.api.java._

//http://qiita.com/shunsukeaihara/items/1524b66579e91d1cf7cf
object SqlLog {
	def main(args: Array[String]) {
		val conf = new SparkConf().setAppName("SparkPi").setMaster("yarn-cluster")
		val sc = new SparkContext(conf)  
		
		val data =sc.textFile(args(0)+"/*.gz") // s3n://bucket/dir
		
		val sqlContext = new org.apache.spark.sql.SQLContext(sc)

	}
}