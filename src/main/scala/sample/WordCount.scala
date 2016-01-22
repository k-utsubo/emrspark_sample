package sample

import org.apache.spark._
import org.apache.spark.SparkContext._
object WordCount {
	  	def main(args: Array[String]) {
	  		println("wordcount,args="+args(0)+","+args(1))
			val conf = new SparkConf().setAppName("wordcount").setMaster("yarn-cluster")
			val sc = new SparkContext(conf)

		  	val textFile = sc.textFile(args(0))
			val counts = textFile.flatMap(line => line.split(" "))
		                 .map(word => (word, 1))
		                 .reduceByKey(_ + _)
		    println("counts="+counts.id+","+counts.name)
		    println(counts.toDebugString)
			counts.saveAsTextFile(args(1))
	  	}
}