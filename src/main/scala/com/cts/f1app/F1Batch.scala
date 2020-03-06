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
  val log = Logger.getLogger("F1Batch") 
  
  /** function to read input file */
  def extractInput(sc:SparkContext) ={
    try{
       val lines = sc.textFile(properties.getProperty("spark.f1app.input"))
       lines
    }catch {
      case ex: FileNotFoundException => {
        log.error("InputFile  not found")
        throw ex
      }
    }
  }
  
   /** function that splits each line of the input into (name, laptime) tuples. */
  def parseLine(line: String) = {    
    if(line.length() >0){
      // Split by commas
      val fields = line.split(",")
      val name = fields(0)
      val laptime = fields(1).toDouble
      (name, laptime)
    }else{
      throw new Exception("Input Data Not Available")
    }
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
    try{
    loadProperies()    
    // Create a SparkContext using every core of the local machine
    val sc = new SparkContext(properties.getProperty("spark.f1app.mode"), "F1Batch")
   
     //Code for the ETL
    log.info("ETL Started--")    
    val lines = extractInput(sc)
    val rdd = lines.map(parseLine)
    val average = computeAverage(rdd)
    loadData(average,properties.getProperty("spark.f1app.output"))
    log.info("ETL Completed--")
    }catch {
      case unknown: Exception => {
        log.error(s"Unknown exception: $unknown")
        throw unknown
      }
    }
    
  }
  
  def loadProperies()={    
    val automationPropertiesFileURL = getClass.getResource("/F1App.conf")  
    if (automationPropertiesFileURL != null) {
       val source = Source.fromURL(automationPropertiesFileURL)    
       properties.load(source.bufferedReader())
    }else {   
       throw new FileNotFoundException("Properties file cannot be loaded")
    }
  }
   
      
  
    

  
}