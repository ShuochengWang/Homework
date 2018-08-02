package shuocheng

import java.util.Scanner

import org.apache.spark.{SparkConf, SparkContext}

object App {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("spark-homework")
    val sc = new SparkContext(conf)

    val textFile = sc.textFile("hdfs://localhost:9000/dataset/a-letter-to-daughter")
    val internalResult = textFile
      .flatMap(line => line.split("[^a-zA-Z0-9-']"))
      .filter(word => !word.isEmpty)
      .map(word => (word, 1))
      .reduceByKey(_ + _)
      .sortBy(pair => pair._2, false)
      .persist()

    // 统计语料中出现次数最多的10个单词
    val result1 = internalResult.take(10)
    sc.makeRDD(result1).saveAsTextFile("result/result1")

    // 统计语料中出现次数相同的单词,按照出现次数降序排序,并输出所有结果
    val result2 = internalResult
      .map(pair => (pair._2, pair._1))
      .groupByKey()
      .filter(pair => pair._2.size > 1)
      .sortByKey(false)
    result2.saveAsTextFile("result/result2")

    // pause the program to check webUI.
    // val scan = new Scanner(System.in)
    // scan.next()

    sc.stop()
  }
}
