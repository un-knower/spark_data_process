package Label.DoHbase

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Get, HTable, Put, Result}
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.SparkSession

/**

把spark的jar添加依赖，打包时去掉依赖包。完成后放到spark节点上运行命令。

/home/spark2/bin/spark-submit --class spark.dataExecute.Demo_hbase_insert \
--jars /opt/cloudera/parcels/CDH-5.12.1-1.cdh5.12.1.p0.3/lib/hbase/hbase-client.jar,\
/opt/cloudera/parcels/CDH-5.12.1-1.cdh5.12.1.p0.3/lib/hbase/hbase-common.jar,\
/opt/cloudera/parcels/CDH-5.12.1-1.cdh5.12.1.p0.3/lib/hbase/hbase-protocol.jar,\
/opt/cloudera/parcels/CDH-5.12.1-1.cdh5.12.1.p0.3/lib/hbase/hbase-hadoop-compat.jar,\
/opt/cloudera/parcels/CDH-5.12.1-1.cdh5.12.1.p0.3/lib/hbase/lib/htrace-core.jar,\
/opt/cloudera/parcels/CDH-5.12.1-1.cdh5.12.1.p0.3/lib/hbase/lib/metrics-core-2.2.0.jar,\
/opt/cloudera/parcels/CDH-5.12.1-1.cdh5.12.1.p0.3/lib/hbase/hbase-server.jar \
 /home/yimr/sss/sparkDemo.jar

  nohup sh spark-submit --executor-cores 1 --class spark.dataExecute.Demo_dirFile  /home/yimr/sss/sparkDemo.jar > /home/yimr/tmp18.txt &

  --driver-class-path 必须在所有设备上都要有这个路径
  效率：
    空值插入：
    插入：
    查询，计算，插入：

  */
object UpdateUserInfo {
  private val master = "172.16.2.31"
  private val port = "7077"
  private val appName = "easy_1"

  private val hdfs_path = "hdfs://cdh1:8020"

  //private val data_input_user_profile = hdfs_path+"/user/hive/src.db/src_label_user_profile/partition_month=201709/*"
  private val data_input_user_profile = hdfs_path+"/user/yimr/hive_upload/C01002MALABELUSR"

  //private val data_input_user = hdfs_path+"/user/hive/src.db/src_label_user/partition_month=201709/*"
  private val data_input_user = hdfs_path+"/user/yimr/hive_upload/C01001MALABELUSR"

  private val data_output = hdfs_path+"/user/yimr/sss/tmp_hbase_1/"

  def getNowTime():String={
    var now:Date = new Date()
    var  dateFormat:SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    var hehe = dateFormat.format( now )
    hehe
  }

  def main (args: Array[String]) {

    if (args.length !=1 && args(0)< "0") {
      System.err.println(s"""
        |Usage: UpdateUserInfo <yearmoth>
        |  <yearmoth> is string ,format yyyyMM,such as 201711
        |
        |
        """.stripMargin)
      System.exit(1)
    }
    println("=====================Begin====================================")
    println("BeginTime:"+getNowTime())
    println("Update Month:"+args(0))

    val spark = SparkSession
      .builder
      .appName(appName)
      .config("spark.executor.cores","40")
      .config("spark.executor.memory","250g")
      .master(s"spark://$master:$port")

      .getOrCreate()
    val sc = spark.sparkContext

    val hdfs = org.apache.hadoop.fs.FileSystem.get(
      new java.net.URI(hdfs_path), new org.apache.hadoop.conf.Configuration())
    if (hdfs.exists(new Path(data_output)))
      hdfs.delete(new Path(data_output), true)

    /**
     * scala split默认 当被分割字符串是一个空字符串时，但不再保留处于末尾位置的空字符串。 添加-1后才会保存
     *
     * 用户id|在网时长|总出账金额_月|用户价值等级|沃信用分
     */
    val user_profile_rdd = sc.textFile(data_input_user_profile+args(0)+"*").filter(_.split("\\|").length > 50)
      .map(line => (line.split("\\|")(3), //phone number
                    (line.split("\\|")(2),//user_id
                      line.split("\\|")(25),//innet_month
                      line.split("\\|")(26),//month_fee
                      line.split("\\|")(31),//user_star_level
                      line.split("\\|")(32)//
                      )))
      .filter( _._1.toString.nonEmpty)

    /**
     * scala split默认 当被分割字符串是一个空字符串时，但不再保留处于末尾位置的空字符串。 添加-1后才会保存
     *
     * 账期|省分|身份证ID|姓名|性别|年龄|生日|移动电话|固话|宽带账号|星座|出生地
     *
     * 账期|身份证ID|姓名|性别|年龄
     */
    val user_rdd = sc.textFile(data_input_user+args(0)+"*").filter(_.split("\\|", -1).length > 8)
      .map(line => (line.split("\\|", -1)(7),//phone number
                    (line.split("\\|", -1)(0),//month
                      line.split("\\|", -1)(2),//identity number
                      line.split("\\|", -1)(3),//name
                      line.split("\\|", -1)(4),//gender
                      line.split("\\|", -1)(5))))//age
      .filter( _._1.toString.nonEmpty)

    //before:(13258687839,((136,31.1,),(F10B9C5A4A5F64D87980A6B9EF040427,杨先生,01,49)))
    val out_rdd = user_rdd.join(user_profile_rdd)
      .map( x=> (x._1, x._2._1.toString().
                       concat(",").replace("(", "")
                       .replace(")", "")
                       .concat(x._2._2.toString()
                               .replace("(", "")
                               .replace(")", ""))
                       .split(",", -1)
                       .mkString("|")
                 ))

    val result = out_rdd.foreachPartition{x =>
    {
      val tableName = "hbase_serv_ident"
      //val tableName = "hbase_widetable_test"

      //conf.set不能写到HTable下面
      val conf = HBaseConfiguration.create()
      conf.set(TableInputFormat.INPUT_TABLE,tableName)
      conf.set("hbase.zookeeper.quorum","cdh1,cdh3,cdh4")
      conf.set("hbase.zookeeper.property.clientPort","2181")
      conf.addResource("/opt/cloudera/parcels/CDH-5.12.1-1.cdh5.12.1.p0.3/etc/hbase/conf.dist/hbase-site.xml")
      //列簇c1

      val table = new HTable(conf,tableName)
      table.setAutoFlush(false,false)
      table.setWriteBufferSize(3*1024*1024)
      x.foreach{y => {
        val result: Result = table.get(new Get(Bytes.toBytes(y._1)))
        val put = new Put(Bytes.toBytes(y._1))
        //账期|身份证ID|姓名|性别|年龄|用户id|在网时长|总出账金额_月|用户价值等级|沃信用分
        val value = Bytes.toString(result.getValue(Bytes.toBytes("c1"), Bytes.toBytes("value")))
        if(value != null) {
          val month = y._2.split("\\|", -1)(0)
          val innetmonth = y._2.split("\\|", -1)(6)
          val age = y._2.split("\\|", -1)(4)
          val user_credit = y._2.split("\\|", -1)(8)
          val wo_score = y._2.split("\\|", -1)(9)

          // -----lyh add-----end------

          val arpu_old = value.split("\\|", -1)(7).split(",", -1)
          val identy_no_old = value.split("\\|", -1)(1)

          var arpu_new = y._2.split("\\|", -1)(7)
          val identy_no_new = y._2.split("\\|", -1)(1)

          var arr = value.split("\\|", -1)
          if (identy_no_new == identy_no_old ) {
            arpu_new = arpu_old.toList.::(arpu_new).take(3).mkString(",")
          }

          // -----lyh add-----start----
          arr(0) = month
          arr(4) = age
          arr(6) = innetmonth
          arr(8) = user_credit
          arr(9) = wo_score
          // -----lyh add-----end------
          arr(7) = arpu_new

          put.add(Bytes.toBytes("c1"), Bytes.toBytes("value"), Bytes.toBytes(arr.mkString("|")))

        }
        else
        {
          put.add(Bytes.toBytes("c1"), Bytes.toBytes("value"), Bytes.toBytes(y._2))

        }

        table.put(put)
      }
        table.flushCommits
      }
    }
    }

    println("Update Done Time:"+getNowTime())
    println("==================================================================")
  }

}
