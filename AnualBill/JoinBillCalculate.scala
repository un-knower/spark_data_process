package AnualBill

import org.apache.hadoop.fs.Path
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
 * Created by lenovo on 2018/1/18.
 */
object JoinBillCalculate {
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

    var result2016 = new Array[Double](3)
    //result2016(0) = fee_ave
    //val tmparr = sortArray1.take((sortArray1.count/4).toInt+1)
    if (sortArray.count % 4 == 0) {

      result2016(0)=(sortmap.lookup((sortArray.count / 4 -1).toLong).head+sortmap.lookup((sortArray.count /4).toLong).head) *0.5 //fee one quarter
    }
    else {
      result2016(0) = sortmap.lookup((sortArray.count / 4 ).toLong).head
    }

    //val tmparr1 = sortArray1.take((sortArray1.count/2).toInt+1)

    if(sortArray.count % 2 == 1) // median
    {
      result2016(1) = sortmap.lookup((sortArray.count / 2 ).toLong).head
    }
    else
    {
      result2016(1) = (sortmap.lookup((sortArray.count / 2 -1).toLong).head+sortmap.lookup((sortArray.count /2).toLong).head) *0.5
    }

    //val tmparr2 = sortArray1.take((sortArray1.count*3/4).toInt+1)

    if(sortArray.count*3 %4 == 0)
    {
      //result2016(3) = (sortArray(sortArray.length*3 / 4)+sortArray(sortArray.length*3/4 - 1))*0.5
      result2016(2) =(sortmap.lookup((sortArray.count*3 /4 -1).toLong).head+sortmap.lookup((sortArray.count*3 /4).toLong).head) *0.5
    }
    else
    {
      result2016(2) =sortmap.lookup((sortArray.count*3/4).toLong).head
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


    //val voice16_arr =  bill16_rdd.map(x=>x._2._2.toDouble)//.collect()
//    val flux16_arr =  bill16_rdd.map(x=>x._2._4.toDouble)//.collect()
//    val fee16_arr =  bill16_rdd.map(x=>x._2._6.toDouble)//.collect()
//
//
//    val a =calculateNumbers(flux16_arr,spark)
//    val result2016 =  (("ave",a(0)),("oneQ",a(1)),("median",a(2)),("ThirdQ",a(3)))
//
//    val b =calculateNumbers(fee16_arr,spark)
//    val result2016b =  (("ave",b(0)),("oneQ",b(1)),("median",b(2)),("ThirdQ",b(3)))
//    println("flux:"+result2016)
//    println("fee:"+result2016b)

    val joinrdd = bill17_rdd.join(bill16_rdd)
    val pos_rdd_voice = joinrdd.filter(x=>(x._2._1._2.toDouble-x._2._2._2.toDouble)>0).map(x=>x._2._1._2.toDouble-x._2._2._2.toDouble)
    val pos_rdd_flux = joinrdd.filter(x=>(x._2._1._4.toDouble-x._2._2._4.toDouble)>0).map(x=>x._2._1._4.toDouble-x._2._2._4.toDouble)
    val pos_rdd_fee = joinrdd.filter(x=>(x._2._1._6.toDouble-x._2._2._6.toDouble)>0).map(x=>x._2._1._6.toDouble-x._2._2._6.toDouble)

    val neg_rdd_voice = joinrdd.filter(x=>(x._2._1._2.toDouble-x._2._2._2.toDouble)<0).map(x=>x._2._1._2.toDouble-x._2._2._2.toDouble)
    val neg_rdd_flux = joinrdd.filter(x=>(x._2._1._4.toDouble-x._2._2._4.toDouble)<0).map(x=>x._2._1._4.toDouble-x._2._2._4.toDouble)
    val neg_rdd_fee = joinrdd.filter(x=>(x._2._1._6.toDouble-x._2._2._6.toDouble)<0).map(x=>x._2._1._6.toDouble-x._2._2._6.toDouble)

    //////////////////////////////////
//    val pos_ave_fee = pos_rdd_fee.reduce(_+_)/pos_rdd_fee.count()
//    val pos_ave_flux = pos_rdd_flux.reduce(_+_)/pos_rdd_flux.count()
//    val pos_ave_voice = pos_rdd_voice.reduce(_+_)/pos_rdd_voice.count()
//
//    val neg_ave_fee = neg_rdd_fee.reduce(_+_)/neg_rdd_fee.count()
//    val neg_ave_flux = neg_rdd_flux.reduce(_+_)/neg_rdd_flux.count()
//    val neg_ave_voice = neg_rdd_voice.reduce(_+_)/neg_rdd_voice.count()
//
//    val result_ave =  (("pos_ave_fee",pos_ave_fee),("pos_ave_flux",pos_ave_flux),("pos_ave_voice",pos_ave_voice),("neg_ave_fee",neg_ave_fee),("neg_ave_flux",neg_ave_flux),("neg_ave_voice",neg_ave_voice))
//
//    println("========MinusAveAll"+result_ave)

    val pos_voice_out =calculateNumbers(pos_rdd_voice,spark)

    val pos_flux_out =calculateNumbers(pos_rdd_flux,spark)
    val pos_fee_out =calculateNumbers(pos_rdd_fee,spark)

    println("pos_voice_out :"+ pos_voice_out(0)+" "+pos_voice_out(1)+" "+pos_voice_out(2))
    println("pos_flux_out "+ pos_flux_out(0)+" "+pos_flux_out(1)+" "+pos_flux_out(2))
    println("pos_fee_out "+ pos_fee_out(0)+" "+pos_fee_out(1)+" "+pos_fee_out(2))

    val neg_voice_out =calculateNumbers(neg_rdd_voice,spark)
    val neg_flux_out =calculateNumbers(neg_rdd_flux,spark)
    val neg_fee_out =calculateNumbers(neg_rdd_fee,spark)

    println("neg_voice_out "+  neg_voice_out(0)+" "+neg_voice_out(1)+" "+neg_voice_out(2))
    println("neg_flux_out "+ neg_flux_out(0)+" "+neg_flux_out(1)+" "+neg_flux_out(2))
    println("neg_fee_out "+ neg_fee_out(0)+" "+neg_fee_out(1)+" "+neg_fee_out(2))



  }
}

