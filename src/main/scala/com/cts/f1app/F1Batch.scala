package com.cts.f1app

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._
import org.apache.spark.rdd.RDD
import java.io.FileNotFoundException
import scala.io.Source
import java.util.Properties
import scala.io.Source.fromFile

/** Driver program to Compute the average Lap time of each F1 Race Driver */
object F1Batch {
  
  val properties: Properties = new Properties()
   /** A function that splits a line of input into (name, laptime) tuples. */
  def parseLine(line: String) = {
      // Split by commas
      val fields = line.split(",")
      val name = fields(0)
      val laptime = fields(1).toDouble
      (name, laptime)
  }
  
  
   /** A function that compute the average laptime */
  def computeAverage(rdd:RDD[(String,Double)]) = {
      val totalsByName = rdd.mapValues(x => (x, 1)).reduceByKey( (x,y) => (x._1 + y._1, x._2 + y._2))    
      val averagesByName = totalsByName.mapValues(x => x._1 / x._2)
      averagesByName
  }
  
  
  /** A function that load the output data */
  def loadData(results:RDD[(String,Double)],outputPath:String) = {
       results.sortBy(_._2).saveAsTextFile(outputPath)
  }
  
  /** Main function to compute the average */
  def main(args: Array[String]) {   
    
    val log = Logger.getLogger("F1Batch")   
    
    // Create a SparkContext using every core of the local machine
    val sc = new SparkContext("local[*]", "F1Batch")
    
     //Code for the ETL
    log.info("ETL Started--")    
    val lines = sc.textFile("file:/home/hduser/sparkdata/input")   
    val rdd = lines.map(parseLine)
    val average = computeAverage(rdd)
    loadData(average,"file:/home/hduser/sparkdata/output")
    log.info("ETL Completed--")
    
  }
  
    

  
}