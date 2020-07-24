package com.spark.training

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object SimpleJson {

  def main(args: Array[String]) {

    // create Spark context with Spark configuration
    val sc = new SparkContext(new SparkConf().setAppName("Spark WordCount").setMaster("local[4]"))

//    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val sqlContext = new org.apache.spark.sql.HiveContext(sc)
        
    val input = args(0)   // i/p file path C:\Users\Admin\Desktop\sparktraining\input\jsonfile.json
    
    val jsonDf = sqlContext.read.json(input)

    jsonDf.collect.foreach(println)
    
    jsonDf.registerTempTable("jsonExtract")

    val res = sqlContext.sql("select * from jsonExtract")
    res.show();

    jsonDf.printSchema() 
    jsonDf.show();
    
//    jsonDf.write.mode("overwrite").saveAsTable("database.tableName")
    
      empdep20df.write.mode("Append").saveAsTable("training.dept10")

//    jsonDf.write.format("parquet").mode("Append").saveAsTable("training_db.tableName")


	
    sc.stop()
  }

}