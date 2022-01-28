package org.example

import org.apache.commons.cli.{BasicParser, Options}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.immutable._
import scala.collection.mutable.ArrayBuffer

object DistCp {
    def main(args: Array[String]) = {

        val sparkConf = new SparkConf().setMaster("local").setAppName(this.getClass.getSimpleName)
        val sc = new SparkContext(sparkConf)
        sc.setLogLevel("WARN")

        val input = "/tmp/test/spark/input"
        val output = "/tmp/test/spark/output"

        // options
        val options = new Options()
        options.addOption("i", "ignore failure", false,"ignore failure")
        options.addOption("m","max concurrence", true,"max concurrence")
        val cmd = new BasicParser().parse(options,args)

        val IGNORE_FAILURE = cmd.hasOption("i")
        val MAX_CONNCURRENCES = if(cmd.hasOption("m")) cmd.getOptionValue("m").toInt
        else 2


        val fs = FileSystem.get(sc.hadoopConfiguration)
        val fileList = fs.listFiles(new Path(input), true)

        val arrayBuffer = ArrayBuffer[String]()
        while(fileList.hasNext) {
            val path = fileList.next().getPath.toString
            arrayBuffer.append(path)
            println(path)
        }

        val rdd = sc.parallelize(arrayBuffer.toSeq,MAX_CONNCURRENCES)
        rdd.foreachPartition(it => {
            val conf = new Configuration()
            val sfs = FileSystem.get(conf)

             while(it.hasNext) {
                val src = it.next()
                val target = src.replace(input,output)
                try {
                    printf("src %s, target %s",src,target)
                    FileUtil.copy(sfs, new Path(src), sfs, new Path(target), false,conf)
                } catch {
                    case ex: Exception =>
                        if (IGNORE_FAILURE) println("ignore failure when copy")
                        else throw ex
                }
            }
        })
    }
}
