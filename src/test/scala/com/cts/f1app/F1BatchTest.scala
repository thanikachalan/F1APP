package com.cts.f1app

import org.junit.runner.RunWith
import org.scalatest.{ BeforeAndAfterAll, FunSuite }
import org.apache.spark.SparkContext 
import org.apache.spark.SparkConf

/**
 * Requires ScalaTest and JUnit4.
 */

class F1BatchTest extends FunSuite  with BeforeAndAfterAll {  
  
  private var sparkConf: SparkConf = _ 
  private var sc: SparkContext = _ 
  
  
  override def beforeAll() { 
    sparkConf = new SparkConf().setAppName("F1BatchTest").setMaster("local") 
    sc = new SparkContext(sparkConf) 
  } 
  
  test("Test the Extraction!!") {
    val lines = sc.textFile("file:/home/hduser/sparkdata/input")   
    val rdd = lines.map(F1Batch.parseLine)
    assert(rdd.take(1).length === 1)
    
  }

  test("Test the Transformation") {
    val lines = sc.textFile("file:/home/hduser/sparkdata/input")   
    val rdd = lines.map(F1Batch.parseLine)
    val average = F1Batch.computeAverage(rdd)
     assert(average.collect().sortBy(_._2).head._2 === 4.526666666666666)
  }

  
  
  override def afterAll() { 
    sc.stop() 
  } 

}