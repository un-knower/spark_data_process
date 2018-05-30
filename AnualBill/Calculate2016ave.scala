package AnualBill

import org.apache.hadoop.fs.Path
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ArrayBuffer

/**
 * Created by lenovo on 2018/1/18.
 */
object Calculate2016ave {

  private val master = "172.16.2.31"
  private val port = "7077"
  private val appName = "billcalculate"

  private val hdfs_path = "hdfs://cdh1:8020"



  private val bill_input = hdfs_path+"/user/yimr/year_end/dwa_user_year_end_final*"

  private val data_output = hdfs_path+"/user/yimr/sss/reslut2016/"

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
    val sortmap = sortArray.zipWithIndex().map{
      case (v, idx) => (idx, v)
    }
     // .collect()

    var result2016 = new Array[Double](4)
    result2016(0) = fee_ave
    //val tmparr = sortArray1.take((sortArray1.count/4).toInt+1)
    if (sortArray.count % 4 == 0) {

      result2016(1) =(sortmap.lookup((sortArray.count / 4 -1).toLong).head+sortmap.lookup((sortArray.count /4).toLong).head) *0.5 //fee one quarter
    }
    else {
      result2016(1) = sortmap.lookup((sortArray.count / 4 ).toLong).head
    }

    //val tmparr1 = sortArray1.take((sortArray1.count/2).toInt+1)

    if(sortArray.count % 2 == 1) // median
    {
      result2016(2) = sortmap.lookup((sortArray.count / 2 ).toLong).head
    }
    else
    {
      result2016(2) = (sortmap.lookup((sortArray.count / 2 -1).toLong).head+sortmap.lookup((sortArray.count /2).toLong).head) *0.5
    }

    //val tmparr2 = sortArray1.take((sortArray1.count*3/4).toInt+1)

    if(sortArray.count*3 %4 == 0)
    {
      //result2016(3) = (sortArray(sortArray.length*3 / 4)+sortArray(sortArray.length*3/4 - 1))*0.5
      result2016(3) =(sortmap.lookup((sortArray.count*3 /4 -1).toLong).head+sortmap.lookup((sortArray.count*3 /4).toLong).head) *0.5
    }
    else
    {
      result2016(3) =sortmap.lookup((sortArray.count*3/4).toLong).head
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
    val bill16_rdd = sc.textFile(bill_input).filter(_.split("\\|", -1).length>6).filter(_.split("\\|", -1)(7).equals("2017"))
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


    //val voice16_arr =  bill16_rdd.map(x=>x._2._2.toDouble)//.collect()
    val flux16_arr =  bill16_rdd.map(x=>x._2._4.toDouble)//.collect()
    val fee16_arr =  bill16_rdd.map(x=>x._2._6.toDouble)//.collect()


    val a =calculateNumbers(flux16_arr,spark)
    val result2016 =  (("ave",a(0)),("oneQ",a(1)),("median",a(2)),("ThirdQ",a(3)))

    val b =calculateNumbers(fee16_arr,spark)
    val result2016b =  (("ave",b(0)),("oneQ",b(1)),("median",b(2)),("ThirdQ",b(3)))
    println("flux:"+result2016)
    println("fee:"+result2016b)

    // out16t = sc.makeRDD(result2016)
    //out16t.repartition(1).saveAsTextFile(hdfs_path+"/user/yimr/year_end/reslut2016")


  }
}
