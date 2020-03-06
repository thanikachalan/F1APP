package com.cts.f1app

import org.junit.runner.RunWith
import org.scalatest.{ BeforeAndAfterAll, FunSuite }
import org.apache.spark.SparkContext 
import org.apache.spark.SparkConf
import java.nio.file.{Paths, Files}

/**
 * Requires ScalaTest and JUnit4.
 */

class F1BatchTest extends FunSuite  with BeforeAndAfterAll {  
  
  private var sparkConf: SparkConf = _ 
  private var sc: SparkContext = _ 
  
  
  override def beforeAll() { 
    sparkConf = new SparkConf().setAppName("F1BatchFileCheckTest").setMaster("local").set("spark.hadoop.validateOutputSpecs", "false").set("spark.driver.allowMultipleContexts","true")
    sc = new SparkContext(sparkConf) 
  } 
  
  test("Test the Average Computation!!") {
    F1Batch.loadProperies()
    val lines = F1Batch.extractInput(sc)  
    val rdd = lines.map(F1Batch.parseLine)
    val average = F1Batch.computeAverage(rdd)
    assert(average.collect().sortBy(_._2).head._2 === 4.526666666666666)
    
  }

  test("Test the output file genartion") {
    F1Batch.loadProperies()
    val lines = F1Batch.extractInput(sc)  
    val rdd = lines.map(F1Batch.parseLine)
    val average = F1Batch.computeAverage(rdd)
    F1Batch.loadData(average,F1Batch.properties.getProperty("spark.f1app.output"))
     assert(Files.exists(Paths.get("/tmp")))
  }

  
  
  override def afterAll() { 
    sc.stop() 
  } 

}