import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark._

object SparkWordCount {
   def main(args: Array[String]) {
      val sc = new SparkContext( "local", "SparkWordCount", "/home/hadoop/apache-spark/latest/", Nil, Map(), Map())
      val input = sc.textFile("/home/pradeep/Documents/Books/Spark/data-input-spark.txt")             
      val counts = input.flatMap(line => line.split(" ")).map(word => (word, 1)).reduceByKey(_ + _)       
      counts.saveAsTextFile("outfile")
      System.out.println("OK");
   }
}

