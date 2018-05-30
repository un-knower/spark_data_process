package AnualBill

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession

/**
 * Created by lenovo on 2018/1/19.
 */
object UserPercentageCount {

  private val master = "172.16.2.31"
  private val port = "7077"
  private val appName = "count_user"

  private val hdfs_path = "hdfs://cdh1:8020"


  private val bill_input = hdfs_path+"/user/yimr/year_end/dwa_user_year_end_final_*"

  private val data_output = hdfs_path+"/user/yimr/sss/tmp_hbase_1/"

  def main (args: Array[String]) {

    val spark = SparkSession
      .builder
      .appName(appName)
      .config("spark.executor.cores", "40")
      .config("spark.executor.memory", "250g")
      .master(s"spark://$master:$port")

      .getOrCreate()
    val sc = spark.sparkContext

    val hdfs = org.apache.hadoop.fs.FileSystem.get(
      new java.net.URI(hdfs_path), new org.apache.hadoop.conf.Configuration())
    if (hdfs.exists(new Path(data_output)))
      hdfs.delete(new Path(data_output), true)


    //输出格式：(用户手机号，())
    val bill16_rdd = sc.textFile(bill_input).filter(_.split("\\|", -1).length > 6).filter(_.split("\\|", -1)(7).equals("2016"))
      .map(x => (x.split("\\|", -1)(0),
      (x.split("\\|", -1)(1),
        x.split("\\|", -1)(2),
        x.split("\\|", -1)(3),
        x.split("\\|", -1)(4),
        x.split("\\|", -1)(5),
        x.split("\\|", -1)(6)))).filter(_._1.toString.nonEmpty)
    //      .filter(_._2._2.toString.nonEmpty)
    //      .filter(_._2._4.toString.nonEmpty)
    //      .filter(_._2._6.toString.nonEmpty)

    val bill17_rdd = sc.textFile(bill_input).filter(_.split("\\|", -1).length > 6).filter(_.split("\\|", -1)(7).equals("2017"))
      .map(x => (x.split("\\|", -1)(0),
      (x.split("\\|", -1)(1),
        x.split("\\|", -1)(2),
        x.split("\\|", -1)(3),
        x.split("\\|", -1)(4),
        x.split("\\|", -1)(5),
        x.split("\\|", -1)(6)))).filter(_._1.toString.nonEmpty)
    //      .filter(_._2._2.toString.nonEmpty)
    //      .filter(_._2._4.toString.nonEmpty)
    //      .filter(_._2._6.toString.nonEmpty)


    val filter16_rdd= bill16_rdd.filter(!_._1.toString.startsWith("0")).filter(!_._1.toString.startsWith("14"))
    val filter17_rdd= bill17_rdd.filter(!_._1.toString.startsWith("0")).filter(!_._1.toString.startsWith("14"))

    println("filtered16_rdd count: "+filter16_rdd.count())
    println("filtered17_rdd count: "+filter17_rdd.count())

    val nonEmp16_rdd = filter16_rdd.filter(_._2._1.length>0).filter(_._2._2.length>0)
      .filter(_._2._3.length>0).filter(_._2._4.length>0).filter(_._2._5.length>0).filter(_._2._6.length>0)

    val nonEmp17_rdd = filter17_rdd.filter(_._2._1.length>0).filter(_._2._2.length>0)
      .filter(_._2._3.length>0).filter(_._2._4.length>0).filter(_._2._5.length>0).filter(_._2._6.length>0)

   // println("nonEmp16_rdd count: "+nonEmp16_rdd.count())
   // println("nonEmp17_rdd count: "+nonEmp17_rdd.count())

   // println("filtered Join user count: "+filter16_rdd.join(filter17_rdd).count())
    println("nonEmp Join user count: "+nonEmp16_rdd.join(nonEmp17_rdd).count())


  }


}
