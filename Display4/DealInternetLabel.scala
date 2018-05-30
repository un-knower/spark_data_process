package Display4

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession

/**
 * Created by lenovo on 2018/2/28.
 */
object DealInternetLabel {
  private val master = "172.16.2.31"
  private val port = "7077"
  private val appName = "display4"

  private val hdfs_path = "hdfs://cdh1:8020"

  private val data_internet = hdfs_path+"/user/yimr/hive_upload/C01003MALABELUSR2017110000000.011"
  private val data_userinfo = hdfs_path+"/user/yimr/hive_upload/C01001MALABELUSR201711*"

  private val data_userprofile = hdfs_path+"/user/yimr/hive_upload/C01002MALABELUSR201711*"

  private val data_output = hdfs_path+"/user/yimr/lyh/internet"

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

    val user_rdd = sc.textFile(data_userinfo).filter(_.split("\\|", -1).length > 8)
      .map(line => (line.split("\\|", -1)(7), //手机号
      (
        line.split("\\|", -1)(4), //性别
        line.split("\\|", -1)(5) //年龄
        )))
      .filter(_._1.toString.nonEmpty)

    val telecom_rdd = sc.textFile(data_userprofile).filter(_.split("\\|", -1).length > 54)
      .map(line => (line.split("\\|",-1)(3), //phone number
      (
        line.split("\\|",-1)(6),//终端偏好
        line.split("\\|",-1)(11),//套餐类型
        line.split("\\|",-1)(26),//出账金额
        line.split("\\|",-1)(27),//当月流量
        line.split("\\|",-1)(29),//职业
        line.split("\\|",-1)(31),//用户星级
        line.split("\\|",-1)(42),//缴费方式偏好
        line.split("\\|",-1)(43)//上网时间偏好
        )// tuple 的_2
      ))
      .filter( _._1.toString.nonEmpty)

    val internet_rdd = sc.textFile(data_internet).filter(_.split("\\|", -1).length > 9).filter(_.split("\\|", -1)(9).equals("011"))
      .map(line => (line.split("\\|",-1)(1), //phone number
      (
        line.split("\\|",-1)(4),//appbID
        line.split("\\|",-1)(5),//app name
        line.split("\\|",-1)(6),//flux
        line.split("\\|",-1)(7),//duration
        line.split("\\|",-1)(8)//PV visit times
        )// tuple 的_2
      ))
      .filter( _._1.toString.nonEmpty)

    val join_rdd = internet_rdd.join(user_rdd).map(x=>(
      x._1,
      x._2._1.toString().concat(",").replace("(", "").replace(")", "")
        .concat(x._2._2.toString.replace("(", "").replace(")", ""))
      ))

    val out_rdd = join_rdd.join(telecom_rdd).map(x=>(
      x._1,
      x._2._1.toString().concat(",").replace("(", "").replace(")", "")
        .concat(x._2._2.toString.replace("(", "").replace(")", "")).split(",",-1).mkString("|")
      )).map(x=>(x.toString().replace("(", "").replace(")", "").replace(",","|")))

    out_rdd.repartition(1).saveAsTextFile(data_output)



  }

}
