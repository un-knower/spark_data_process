package AnualBill

/**
 * Created by lenovo on 2018/1/17.
 */

import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Get, HTable, Put, Result}
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.rdd.RDD

import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ArrayBuffer

object TestCalculate {

  //四分位数、中位数计算方法 ：http://blog.163.com/xshw2007@126/blog/static/10979236420133654318616/


  private val master = "172.16.2.31"
  private val port = "7077"
  private val appName = "billcalculate"

  private val hdfs_path = "hdfs://cdh1:8020"



  private val bill_input = hdfs_path+"/user/yimr/year_end/dwa_user_year_end_final*"

  private val data_output = hdfs_path+"/user/yimr/sss/tmp_bill_1/"

  def calculateNumbers(rdd:RDD[Double],spark:SparkSession):Array[Double]={

//    val spark = SparkSession
//      .builder
//      .appName(appName)
//      .config("spark.executor.cores", "40")
//      .config("spark.executor.memory", "250g")
//      .master(s"spark://$master:$port")
//      .getOrCreate()
    val sc = spark.sparkContext

    //val tmprdd = sc.makeRDD(oriArray)
    //val rmprdd1 = rdd.map(x=>x).reduce(_+_)
    val fee_ave = rdd.reduce(_ + _) / rdd.count()
    val sortArray = rdd.sortBy(x => x)
      .collect()
    //val sortArray1 = rdd.sortBy(x => x)


    var result2016 = new Array[Double](4)
    result2016(0) = fee_ave
    //val tmparr = sortArray1.take((sortArray1.count/4).toInt+1)
    if (sortArray.length % 4 == 0) {

      result2016(1) = (sortArray(sortArray.length / 4) + sortArray(sortArray.length / 4 - 1)) * 0.5 //fee one quarter
    }
    else {
      result2016(1) = sortArray(sortArray.length/ 4)
    }

    //val tmparr1 = sortArray1.take((sortArray1.count/2).toInt+1)

    if(sortArray.length % 2 == 1) // median
    {
      result2016(2) = sortArray(sortArray.length / 2)
    }
    else
    {
      result2016(2) = (sortArray(sortArray.length / 2)+sortArray((sortArray.length / 2)-1)) *0.5
    }

    //val tmparr2 = sortArray1.take((sortArray1.count*3/4).toInt+1)

    if(sortArray.length*3 %4 == 0)
    {
      result2016(3) = (sortArray(sortArray.length*3 / 4)+sortArray(sortArray.length*3/4 - 1))*0.5
    }
    else
    {
      result2016(3) = sortArray(sortArray.length*3 / 4)
    }

    return result2016

  }

  def main (args: Array[String]) {

    val spark = SparkSession
      .builder
      .appName(appName)
      .config("spark.executor.cores", "40")
      .config("spark.executor.memory", "250g")
      .config("spark.driver.memory", "20g")
      .config("spark.driver.extraJavaOptions", "-XX:+UseG1GC")
      .config("spark.executor.extraJavaOptions", "-XX:+UseG1GC")
      .config("spark.sql.autoBroadcastJoinThreshold", "-1")
      .config("spark.default.parallelism", "2000")
      .config("spark.shuffle.file.buffer", "128k")
      .master(s"spark://$master:$port")
      .getOrCreate()
    val sc = spark.sparkContext

    val hdfs = org.apache.hadoop.fs.FileSystem.get(
      new java.net.URI(hdfs_path), new org.apache.hadoop.conf.Configuration())
    if (hdfs.exists(new Path(data_output)))
      hdfs.delete(new Path(data_output), true)

    if(!hdfs.exists(new Path(hdfs_path+"/user/yimr/year_end/dwa_user_year_end_final_010.txt")))
    {
      println("Can not find any file")
      return
    }


    //输出格式：(用户手机号，())
    val bill16_rdd = sc.textFile(bill_input).filter(_.split("\\|", -1).length>6).filter(_.split("\\|", -1)(7).equals("2016"))
      .map(x => (x.split("\\|", -1)(0),
      (x.split("\\|", -1)(1),
        x.split("\\|", -1)(2),
        x.split("\\|", -1)(3),
        x.split("\\|", -1)(4),
        x.split("\\|", -1)(5),
        x.split("\\|", -1)(6))))
    .filter(_._2._2.toString.nonEmpty)
    .filter(_._2._4.toString.nonEmpty)
    .filter(_._2._6.toString.nonEmpty)
    val bill17_rdd = sc.textFile(bill_input).filter(_.split("\\|", -1).length>6).filter(_.split("\\|", -1)(7).equals("2017"))
      .map(x => (x.split("\\|", -1)(0),
      (x.split("\\|", -1)(1),
        x.split("\\|", -1)(2),
        x.split("\\|", -1)(3),
        x.split("\\|", -1)(4),
        x.split("\\|", -1)(5),
        x.split("\\|", -1)(6))))
      .filter(_._2._2.toString.nonEmpty)
      .filter(_._2._4.toString.nonEmpty)
      .filter(_._2._6.toString.nonEmpty)

    var debug =0

    if(debug==1)
    {
      bill16_rdd.saveAsTextFile(hdfs_path+"/user/yimr/year_end/debug")
      return
    }




    val voice16_arr =  bill16_rdd.map(x=>x._2._2.toDouble)//.collect()
    val flux16_arr =  bill16_rdd.map(x=>x._2._4.toDouble)//.collect()
    val fee16_arr =  bill16_rdd.map(x=>x._2._6.toDouble)//.collect()

    val result2016 =  ArrayBuffer[Double]()
    result2016 ++= calculateNumbers(voice16_arr,spark)

    //val out16t = sc.parallelize(result2016)
    //out16t.saveAsTextFile(hdfs_path+"/user/yimr/year_end/reslut2016")
    //return Unit

    result2016 ++= calculateNumbers(flux16_arr,spark)
    result2016 ++= calculateNumbers(fee16_arr,spark)

    //val out16t = sc.parallelize(result2016)
    //out16t.saveAsTextFile(hdfs_path+"/user/yimr/year_end/reslut2016voice")
    //return Unit

    val voice17_arr =  bill17_rdd.map(x=>x._2._2.toDouble)//.collect()
    val flux17_arr =  bill17_rdd.map(x=>x._2._4.toDouble)//.collect()
    val fee17_arr =  bill17_rdd.map(x=>x._2._6.toDouble)//.collect()

    val result2017 =  ArrayBuffer[Double]()
    result2017 ++= calculateNumbers(voice17_arr,spark)
    result2017 ++= calculateNumbers(flux17_arr,spark)
    result2017 ++= calculateNumbers(fee17_arr,spark)

    // 输出为 voice flux fee 的顺序，每个一个大类下内部顺序为ave 1/4,median,3/4
    val out16 = sc.parallelize(result2016)
    out16.saveAsTextFile(hdfs_path+"/user/yimr/year_end/reslut2016")

    val out17 = sc.parallelize(result2017)
    out17.saveAsTextFile(hdfs_path+"/user/yimr/year_end/reslut2017")

    val joinrdd = bill17_rdd.join(bill16_rdd)
    val pos_rdd_voice = joinrdd.filter(x=>(x._2._1._2.toDouble-x._2._2._2.toDouble)>0).map(x=>x._2._1._2.toDouble-x._2._2._2.toDouble)
    val pos_rdd_flux = joinrdd.filter(x=>(x._2._1._4.toDouble-x._2._2._4.toDouble)>0).map(x=>x._2._1._4.toDouble-x._2._2._4.toDouble)
    val pos_rdd_fee = joinrdd.filter(x=>(x._2._1._6.toDouble-x._2._2._6.toDouble)>0).map(x=>x._2._1._6.toDouble-x._2._2._6.toDouble)

    val neg_rdd_voice = joinrdd.filter(x=>(x._2._1._2.toDouble-x._2._2._2.toDouble)<0).map(x=>x._2._1._2.toDouble-x._2._2._2.toDouble)
    val neg_rdd_flux = joinrdd.filter(x=>(x._2._1._4.toDouble-x._2._2._4.toDouble)<0).map(x=>x._2._1._4.toDouble-x._2._2._4.toDouble)
    val neg_rdd_fee = joinrdd.filter(x=>(x._2._1._6.toDouble-x._2._2._6.toDouble)<0).map(x=>x._2._1._6.toDouble-x._2._2._6.toDouble)

    val pos_ave_fee = pos_rdd_fee.reduce(_+_)/pos_rdd_fee.count()
    val pos_ave_flux = pos_rdd_flux.reduce(_+_)/pos_rdd_flux.count()
    val pos_ave_voice = pos_rdd_voice.reduce(_+_)/pos_rdd_voice.count()

    val neg_ave_fee = neg_rdd_fee.reduce(_+_)/neg_rdd_fee.count()
    val neg_ave_flux = neg_rdd_flux.reduce(_+_)/neg_rdd_flux.count()
    val neg_ave_voice = neg_rdd_voice.reduce(_+_)/neg_rdd_voice.count()


    val sort_pos_fee = pos_rdd_fee.sortBy(x=>x)
    val sort_pos_flux = pos_rdd_flux.sortBy(x=>x)
    val sort_pos_voice = pos_rdd_voice.sortBy(x=>x)
    val sort_neg_fee = neg_rdd_fee.sortBy(x=>x)
    val sort_neg_flux = neg_rdd_flux.sortBy(x=>x)
    val sort_neg_voice = neg_rdd_voice.sortBy(x=>x)

    val pn_result = new Array[Double](12)
    pn_result(0) = pos_ave_voice
    pn_result(1) = pos_ave_flux
    pn_result(2) = pos_ave_fee
    pn_result(3) = neg_ave_voice
    pn_result(4) = neg_ave_flux
    pn_result(5) = neg_ave_fee



    if(sort_pos_fee.count() % 2 == 1) // median
    {
      pn_result(8) = sort_pos_fee.collect()((sort_pos_fee.count()/2).toInt)
    }
    else
    {
      pn_result(8) = (sort_pos_fee.collect()((sort_pos_fee.count()/2).toInt)+sort_pos_fee.collect()((sort_pos_fee.count()/2).toInt-1)) *0.5
    }

    if(sort_pos_flux.count() % 2 == 1) // median
    {
      pn_result(7) = sort_pos_flux.collect()((sort_pos_flux.count()/2).toInt)
    }
    else
    {
      pn_result(7) = (sort_pos_flux.collect()((sort_pos_flux.count()/2).toInt)+sort_pos_flux.collect()((sort_pos_flux.count()/2).toInt-1)) *0.5
    }

    if(sort_pos_voice.count() % 2 == 1) // median
    {
      pn_result(6) = sort_pos_voice.collect()((sort_pos_voice.count()/2).toInt)
    }
    else
    {
      pn_result(6) = (sort_pos_voice.collect()((sort_pos_voice.count()/2).toInt)+sort_pos_voice.collect()((sort_pos_voice.count()/2).toInt-1)) *0.5
    }

    ////////////////////////////////////////////////////////////////////////

    if(sort_neg_fee.count() % 2 == 1) // median
    {
      pn_result(11) = sort_neg_fee.collect()((sort_neg_fee.count()/2).toInt)
    }
    else
    {
      pn_result(11) = (sort_neg_fee.collect()((sort_neg_fee.count()/2).toInt)+sort_neg_fee.collect()((sort_neg_fee.count()/2).toInt-1)) *0.5
    }

    if(sort_neg_flux.count() % 2 == 1) // median
    {
      pn_result(10) = sort_neg_flux.collect()((sort_neg_flux.count()/2).toInt)
    }
    else
    {
      pn_result(10) = (sort_neg_flux.collect()((sort_neg_flux.count()/2).toInt)+sort_neg_flux.collect()((sort_neg_flux.count()/2).toInt-1)) *0.5
    }

    if(sort_neg_voice.count() % 2 == 1) // median
    {
      pn_result(9) = sort_neg_voice.collect()((sort_neg_voice.count()/2).toInt)
    }
    else
    {
      pn_result(9) = (sort_neg_voice.collect()((sort_neg_voice.count()/2).toInt)+sort_neg_voice.collect()((sort_neg_voice.count()/2).toInt-1)) *0.5
    }


    sc.parallelize(pn_result).saveAsTextFile(hdfs_path+"/user/yimr/year_end/reslut_pos_neg")


    /*
      val tmprdd1 = sc.makeRDD(fluxArray)
      val sortfluxArray = tmprdd1.sortBy(x => x).collect()
      result2016(4) = tmprdd1.reduce(_ + _) / tmprdd1.count()
      if (fluxArray.length % 4 == 0) {
        result2016(5) = (sortfluxArray(fluxArray.length / 4) + sortfluxArray(fluxArray.length / 4 - 1)) * 0.5 //fee one quarter

      }
      else {
        result2016(5) = sortfluxArray(fluxArray.length/ 4)
      }

      if(fluxArray.length % 2 == 1) // median
      {
        result2016(6) = sortfluxArray(fluxArray.length / 2)
      }
      else
      {
        result2016(6) = (sortfluxArray(fluxArray.length / 2)+sortfluxArray((fluxArray.length / 2)-1)) *0.5
      }
      if(fluxArray.length*3 %4 == 0)
      {
        result2016(7) = (sortfluxArray(fluxArray.length*3 / 4)+sortfluxArray(fluxArray.length*3/4 - 1))*0.5
      }
      else
      {
        result2016(7) = sortfluxArray(fluxArray.length*3 / 4)
      }


      val tmprdd2 = sc.makeRDD(voiceArray)
      val sortvoiceArray = tmprdd2.sortBy(x => x).collect()
      result2016(8) = tmprdd2.reduce(_ + _) / tmprdd2.count()
      if (voiceArray.length % 4 == 0) {
        result2016(9) = (sortvoiceArray(voiceArray.length / 4) + sortvoiceArray(voiceArray.length / 4 - 1)) * 0.5 //fee one quarter

      }
      else {
        result2016(9) = sortvoiceArray(voiceArray.length/ 4)
      }

      if(voiceArray.length % 2 == 1) // median
      {
        result2016(10) = sortvoiceArray(voiceArray.length / 2)
      }
      else
      {
        result2016(10) = (sortvoiceArray(voiceArray.length / 2)+sortvoiceArray((voiceArray.length / 2)-1)) *0.5
      }
      if(voiceArray.length*3 %4 == 0)
      {
        result2016(11) = (sortvoiceArray(voiceArray.length*3 / 4)+sortvoiceArray(voiceArray.length*3/4 - 1))*0.5
      }
      else
      {
        result2016(11) = sortvoiceArray(voiceArray.length*3 / 4)
      }
      */

  }
}
