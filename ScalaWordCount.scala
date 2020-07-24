package com.spark.training

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object ScalaWordCount {

  def main(args: Array[String]) {

    // create Spark context with Spark configuration
    val sc = new SparkContext(new SparkConf().setAppName("Spark WordCount").setMaster("local[4]"))

    //Load inputFile
    val input = args(0)   // i/p file path C:\Users\Admin\Desktop\sparktraining\input\wordcountinput.txt
    val output = args(1)  // o/p file path C:\Users\Admin\Desktop\sparktraining\output\wordcountoutput

    val inputData = sc.textFile(input)
    inputData.collect.foreach(println)

    val counts = inputData.flatMap(line => line.split(" ")).map(word => (word, 1))
        .reduceByKey(_ + _)
        
//    reduceByKey((a, b) => a + b)
        
    counts.foreach(println)
    counts.saveAsTextFile(output)

    sc.stop()
  }

}