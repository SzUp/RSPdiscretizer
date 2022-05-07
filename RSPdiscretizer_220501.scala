package com.qcut

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import scala.math.sqrt
import java.util.Date
import scala.util.Random

object quct3 {
  def main(args: Array[String]): Unit = {
    //计算二范数
    def calculate_norm_2(array1:Array[Double],array2: Array[Double]):Double={
        var count :Double= 0;
        for(i <- array1.indices){
          count += sqrt((array1(i)-array2(i))*(array1(i)-array2(i)))
        }
      count
    }
    //生成随机数0~maximum，参数1：个数，参数2：最大值
    def randomNew(nums:Int,maximun:Int)={
      var resultList:List[Int]=Nil
      while(resultList.length<nums){
        val randomNum=(new Random).nextInt(maximun)
        if(!resultList.exists(s=>s==randomNum)){
          resultList=resultList:::List(randomNum)
        }
      }
      resultList
    }
    def get_quantile(maximun:Long,bucket_num:Int): Array[Double] ={
      var gap_list = new Array[Double](bucket_num-1)
      for (i <- gap_list.indices){
        gap_list(i) = ((i+1.0)/bucket_num)*maximun
      }
      println("this is gaplist")
      println(gap_list.mkString("Array(", ", ", ")"))
      gap_list
    }

    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
    val spark = SparkSession.builder().master("local[*]").appName("rank").getOrCreate()
    //val spark = SparkSession.builder().appName("rank").getOrCreate()
    val sc: SparkContext = spark.sparkContext
    sc.setLogLevel("error")
    val scam: StructType = StructType(StructField("id",StringType)::Nil)

    // parameters
    val filename = "100000000_randnum.txt"
    //val filename1 = "../../" + filename
    val filename1 = "./" + filename
    val bucket_num = 10
    sc.broadcast(bucket_num)
    //val inputRDD: RDD[Double] = sc.textFile(filename1)
    val inputRDD: RDD[Double] = sc.textFile(filename1,24).map(_.toDouble)
    val sample_rate = 1.0
    val used_block_nums = (inputRDD.getNumPartitions * sample_rate).toInt
    val len:Long = filename.split("_")(0).toLong
    println(inputRDD.getNumPartitions)

    val choosed_index = randomNew(used_block_nums.toInt,inputRDD.getNumPartitions)
    println(choosed_index)
    sc.broadcast(choosed_index)
//    inputRDD.map(t => (_,""))

    val outputRDD = inputRDD.mapPartitionsWithIndex((block_index,it) => {
      if(choosed_index.contains(block_index)){
        val tmp = it.toArray.sorted
        val tmp_buck = bucket_num.intValue()
        var gap_list = new Array[Double](bucket_num-1)
        for(i <- 1 to bucket_num-1){
          val index = ((tmp.length)*(i/bucket_num.toDouble)).toInt
          gap_list(i-1) = tmp(index)
        }
        gap_list.toIterator
      }
      else {
        Nil.iterator
      }
    })

    var start_time =new Date().getTime
    val mid_res = outputRDD.collect()
    var end_time = new Date().getTime
    println((end_time - start_time) + "ms")
    //全量数据使用该算法
      var divided_num = Array.ofDim[Double](used_block_nums, bucket_num-1)
      for(i <- 0 until(used_block_nums)){
        divided_num(i) = mid_res.slice(i*(bucket_num-1),(i+1)*(bucket_num-1))
      }
      val gap_list = new Array[Double](bucket_num-1)//保存最终结果
      var count = 0
      divided_num.foreach(it => {
        val mylist = it
        for (i <- it.indices){
          gap_list(i) += divided_num(count)(i)
        }
        count = count + 1
      })

      for(i <- gap_list.indices) {
          gap_list(i) = gap_list(i)/used_block_nums
      }
    end_time = new Date().getTime
      println((end_time - start_time) + "ms")
      println(gap_list.mkString("Array(", ", ", ")"))
    val standard = get_quantile(len, bucket_num)
    println(standard.mkString("Array(", ", ", ")"))
    println(calculate_norm_2(standard,gap_list))
  }

}