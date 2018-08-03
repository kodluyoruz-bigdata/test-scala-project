package me.ycan

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

import scala.util.Try

case class RawData(a: String, b: String, c: Any)
case class GroupedMyMotherName(mother: String, count: Long){
  override def toString(): String= s"Mother name: $mother, Count: $count"
}

object ExampleSparkApp extends App{

  val conf = new SparkConf()
    .setMaster("local[2]")
    .setAppName("test-app")
  val sparkContext: SparkContext = SparkContext.getOrCreate(conf)

  val rawData: RDD[String] = sparkContext.textFile(path = "/Users/ycan/Desktop/test.txt")

  val readData: RDD[Try[RawData]] =rawData.map{ line =>
    val fields = line.split(",")
    Try(
      RawData(fields(0), fields(1), fields(2))
    )
  }



  val filteredRawData: RDD[RawData] = readData
    .filter(maybeRawData => maybeRawData.isSuccess)
    .map(data => data.get)

  val result = filteredRawData
    .map(r => (r.a, r.c))
    .reduceByKey { case (value1: String, value2: String) => s"$value1 $value2"
    }


//  val result: RDD[GroupedMyMotherName] = filteredRawData
//    .groupBy(_.a).map{ case (key, values) =>
//    GroupedMyMotherName(key, values.size)
//  }

  val collectedResult: Array[(String, String)] = result.collect()

  collectedResult.foreach(println)


  case class A(name: String, surname:String, childName: String)



  val example2Data = sparkContext.textFile(path = "/Users/ycan/Desktop/test.txt")

  val mappedData = example2Data.map{ line =>

    val fields = line.split(";")

    val childNames = fields(2).split(",")

    childNames.map{ child =>
      A(fields(0), fields(1), child)
    }
  }

  val testData = mappedData.flatMap(r => r)

  testData


  case class B(name: String, surname:String, childName: Array[String])


  val testData2 = example2Data.map{ line =>
    val fields = line.split(";")

    val childNames = fields(2).split(",")

    B(fields(0), fields(1), childNames)

  }

  val testData3 = testData2.map{ b =>
    b.childName.map{ child =>
      A(b.name, b.surname, child)
    }
  }

  val testArray = List("a", "b", "b", "c").toArray

  val testRDD = sparkContext.parallelize(testArray)

  testRDD.countByValue()













//    val sparkSession = SparkSession
//      .builder()
//      .appName("my-test-spark-app")
//      .getOrCreate()
//


}
