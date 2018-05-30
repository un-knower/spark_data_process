package AnualBill

import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Put, HTable}
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.SparkSession

/**
 * Created by lenovo on 2018/1/19.
 */
object BillHbaseNew {
  private val master = "172.16.2.31"
  private val port = "7077"
  private val appName = "easy_1"

  private val hdfs_path = "hdfs://cdh1:8020"

  private val data_input_user = hdfs_path+"/user/yimr/hive_upload/C01001MALABELUSR201711*"
  private val data_input_user_profile = hdfs_path+"/user/yimr/hive_upload/C01002MALABELUSR201711*"

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

    val user_rdd = sc.textFile(data_input_user).filter(_.split("\\|", -1).length > 8)
      .map(line => (line.split("\\|", -1)(7), //手机号
      (
        line.split("\\|", -1)(4), //性别
        line.split("\\|", -1)(5) //年龄
        )))
      .filter(_._1.toString.nonEmpty)

    val user_innet_rdd = sc.textFile(data_input_user_profile).filter(_.split("\\|", -1).length > 54)
      .map(line => (line.split("\\|",-1)(3), //phone number
                     (line.split("\\|",-1)(54))// innet date
        ))
      .filter( _._1.toString.nonEmpty)

    //输出格式：(用户手机号，())
    val bill16_rdd = sc.textFile(bill_input).filter(_.split("\\|", -1).length > 6).filter(_.split("\\|", -1)(7).equals("2016"))
      .map(x => (x.split("\\|", -1)(0),
      (x.split("\\|", -1)(1),
        x.split("\\|", -1)(2),
        x.split("\\|", -1)(3),
        x.split("\\|", -1)(4),
        x.split("\\|", -1)(5),
        x.split("\\|", -1)(6)))).filter( _._1.toString.nonEmpty)
    //      .filter(_._2._2.toString.nonEmpty)
    //      .filter(_._2._4.toString.nonEmpty)
    //      .filter(_._2._6.toString.nonEmpty)

    //输出格式：(用户手机号，())
    val bill17_rdd = sc.textFile(bill_input).filter(_.split("\\|", -1).length > 6).filter(_.split("\\|", -1)(7).equals("2017"))
      .map(x => (x.split("\\|", -1)(0),
      (x.split("\\|", -1)(1),
        x.split("\\|", -1)(2),
        x.split("\\|", -1)(3),
        x.split("\\|", -1)(4),
        x.split("\\|", -1)(5),
        x.split("\\|", -1)(6)))).filter( _._1.toString.nonEmpty)
    //      .filter(_._2._2.toString.nonEmpty)
    //      .filter(_._2._4.toString.nonEmpty)
    //      .filter(_._2._6.toString.nonEmpty)


    var debug = 0


    //join后格式：(用户手机号，（（16），（17））)
    // map中的x是指 join后生成的元组，x._1是手机号，x._2是（（16），（17））x._2._1和_2就是16年和17年数据了。
    //val out_rdd = user_rdd.join(user_profile_rdd).map( x=> (x._1, x._2._1.toString().concat(",").replace("(", "").replace(")", "").concat(x._2._2.toString().replace("(", "").replace(")", "")).split(",", -1).mkString("|")
    val join_rdd = bill16_rdd.fullOuterJoin(bill17_rdd).map(x=>(x._1,(x._2._1.toString().concat(",").replace("(", "").replace(")", "").replace("Some","")
      .concat(x._2._2.toString().replace("(", "").replace(")", "")).replace("Some","")
      )
      )
    )
    //(手机号，((),()))
    //手机号,总语音|月均语音|总流量|月均流量|总话费|月均话费|性格|年龄
    val mid_rdd = join_rdd.leftOuterJoin(user_rdd).map( x=>(
      x._1,
      (x._2._1.toString().concat(",").replace("(", "").replace(")", "")
        .concat(x._2._2.toString.replace("(", "").replace(")", "")).replace("Some","").replace("None",","))
      )
    )

    //手机号,总语音|月均语音|总流量|月均流量|总话费|月均话费|性格|年龄|入网时间
    val out_rdd = mid_rdd.leftOuterJoin(user_innet_rdd).map( x=>(
      x._1,
      x._2._1.toString().concat(",").replace("(", "").replace(")", "")
        .concat(x._2._2.toString.replace("(", "").replace(")", "")).replace("Some","").replace("None","").split(",",-1).mkString("|")
      )
    )

    val result = out_rdd.foreachPartition { x => {
      val tableName = "bill_131"
      //conf.set不能写到HTable下面
      val conf = HBaseConfiguration.create()
      conf.set(TableInputFormat.INPUT_TABLE, tableName)
      conf.set("hbase.zookeeper.quorum", "cdh1,cdh3,cdh4")
      conf.set("hbase.zookeeper.property.clientPort", "2181")
      conf.addResource("/opt/cloudera/parcels/CDH-5.12.1-1.cdh5.12.1.p0.3/etc/hbase/conf.dist/hbase-site.xml")
      //列簇c1

      val table = new HTable(conf, tableName)
      table.setAutoFlush(false, false)
      table.setWriteBufferSize(3 * 1024 * 1024)
      x.foreach { y => {

        val put = new Put(Bytes.toBytes(y._1))
        put.add(Bytes.toBytes("col1"), Bytes.toBytes("value"), Bytes.toBytes(y._2))
        table.put(put)
      }
        table.flushCommits
      }
    }
    }





  }

}
