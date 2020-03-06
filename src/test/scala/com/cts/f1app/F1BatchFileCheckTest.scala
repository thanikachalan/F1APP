package com.cts.f1app

import org.junit.runner.RunWith
import org.scalatest.{ BeforeAndAfterAll, FunSuite }
import org.apache.spark.SparkContext 
import org.apache.spark.SparkConf

class F1BatchFileCheckTest extends FunSuite  with BeforeAndAfterAll{
  
  
  private var sparkConf: SparkConf = _ 
  private var sc: SparkContext = _ 
  
  
  override def beforeAll() { 
    sparkConf = new SparkConf().setAppName("F1BatchFileCheckTest").setMaster("local").set("spark.hadoop.validateOutputSpecs", "false").set("spark.driver.allowMultipleContexts","true")   
    sc = new SparkContext(sparkConf) 
  } 
  
  test("Test if the property file  availablity!!") {
    F1Batch.loadProperies()
    assert(F1Batch.properties.size()>0)
    
  }

  test("Test if the input file availablity") {
     F1Batch.loadProperies()
    val lines = F1Batch.extractInput(sc)  
    assert(lines != null)
  }
  
  test("Test if the input file have data") {
    F1Batch.loadProperies()
    val lines = F1Batch.extractInput(sc)  
    val rdd = lines.map(F1Batch.parseLine)
    assert(rdd != null && rdd.take(1).length === 1)
  }
  
}