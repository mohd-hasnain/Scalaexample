package com.spark.training

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.DataFrame
import org.apache.spark.rdd.RDD

object SparkSqlEx1 {

  case class Emp(id: Int, name: String, age: Int, dept: String)

  def main(args: Array[String]) {

    // create Spark context with Spark configuration
    val sc = new SparkContext(new SparkConf().setAppName("Spark SparkSqlEx1").setMaster("local[4]"))

    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val sqlContext = new org.apache.spark.sql.HiveContext(sc)

    val input = args(0)       // i/p file path C:\Users\Admin\Desktop\sparktraining\input\emp.txt

    import sqlContext.implicits._

    val empDf = sc.textFile(input).map(_.split(","))
      .map(e => Emp(e(0).trim.toInt, e(1), e(2).trim.toInt, e(3) )).toDF()

    empDf.registerTempTable("emp")
    
    val dept10Emp = sqlContext.sql("SELECT id, name, age FROM emp WHERE dept = 10")

    dept10Emp.map(e => "Name: " + e(1)).collect().foreach(println)

    dept10Emp.map(e => "Name: " + e.getAs[String]("name")).collect().foreach(println)
      
    sc.stop()
  }
}