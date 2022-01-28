package org.example

import org.apache.spark.{SparkContext,SparkConf}
import org.apache.hadoop.fs.{FileSystem, Path}
import scala.collection.mutable.ArrayBuffer

object InvertIndex extends App {
  // 先根据输入参数构建一个文件列表
  // 根据文件列表构造一个rdd联合rdd
  // 参考 wordCount 构造 词频
  // 最后根据输入的要求我们调整一下格式

  val sparkConf = new SparkConf().setMaster("local").setAppName(this.getClass.getName)
  val sc = new SparkContext(sparkConf)
  sc.setLogLevel("WARN")

  val input = "/tmp/test/spark"
  val fs = FileSystem.get(sc.hadoopConfiguration)

  val fileList = fs.listFiles(new Path(input), true)
  var rdd = sc.emptyRDD[(String, String)] // filename, word

  while(fileList.hasNext) {
      val path = fileList.next
      val fileName = path.getPath.getName
      rdd = rdd.union(sc.textFile(path.getPath.toString)
        .flatMap(_.split("\\s+")).map((fileName,_)))
  }
//  println("---"*10)
//  rdd.foreach(println)
//  println("---"*10)

  val rdd2 = rdd.map((_,1)).reduceByKey(_+_)
//  println("---"*10)
//  rdd2.foreach(println)
//  println("---"*10)

  val rdd3 = rdd2.map(data => (data._1._2,
    String.format("(%s,%s)",data._1._1, data._2.toString)))
    .reduceByKey(_ + "," + _)
    .map(data => String.format("%s, {%s}",data._1,data._2))

  println("---"*10)
  rdd3.foreach(println)
  println("---"*10)
}
