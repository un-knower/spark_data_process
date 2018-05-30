package King

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession

/**
 * Created by lenovo on 2018/2/1.
 */
object KingUserInfo {

  private val master = "172.16.2.31"
  private val port = "7077"
  private val appName = "king"

  private val hdfs_path = "hdfs://cdh1:8020"

  private val custom_info = hdfs_path+"/user/yimr/hive_upload/C01001MALABELUSR2017120000000.011"
  private val user_feature = hdfs_path+"/user/yimr/hive_upload/C01002MALABELUSR2017110000000.011"


  private val data_output = hdfs_path+"/user/yimr/lyh/king"

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

    val cus_rdd = sc.textFile(custom_info).filter(_.split("\\|", -1).length > 8)
      .map(line => (line.split("\\|", -1)(7), //手机号
      (
        line.split("\\|", -1)(4), //性别
        line.split("\\|", -1)(5) //年龄
        )))
      .filter(_._1.toString.nonEmpty)

    val user_rdd = sc.textFile(user_feature).filter(_.split("\\|", -1).length > 54)
      .map(line => (line.split("\\|",-1)(3), //phone number
      (
        line.split("\\|",-1)(10),//当前套餐
        line.split("\\|",-1)(11),//套餐类型
        line.split("\\|",-1)(12),//是否固移融合套餐
        line.split("\\|",-1)(13),//是否主副卡
        line.split("\\|",-1)(20),//操作系统
        line.split("\\|",-1)(23),//是否双卡手机用户
        line.split("\\|",-1)(25),//在网时长
        line.split("\\|",-1)(26),//总出账金额_月
        line.split("\\|",-1)(27),//当月累计-流量
        line.split("\\|",-1)(31),//用户价值等级（用户分类）
        line.split("\\|",-1)(40),//连续超套
        line.split("\\|",-1)(42),//交费方式偏好
        line.split("\\|",-1)(43),//上网时间偏好
        line.split("\\|",-1)(45) //合约类型
        )// tuple 的_2
      ))
      .filter( _._1.toString.nonEmpty)

    val mid_rdd = user_rdd.join(cus_rdd).map(x=>(
      x._1, //上面那个map里面加的1，是为了可以join，这里直接不要了，即x._2._1
      x._2._1.toString().concat(",").replace("(", "").replace(")", "")
      .concat(x._2._2.toString.replace("(", "").replace(")", ""))
      ))

    mid_rdd.repartition(1).saveAsTextFile(data_output)

  }

}
