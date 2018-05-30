package Label.DoHbase

import scala.io.Source


/**
 * Created by lenovo on 2018/1/3.
 */
object testSplit {

  def main(args: Array[String]) {

    val  a= "18413343"
    val  b = (1,2,3,4)
    val ll = (a,b)

    val  d= "18413343"
    val  c = (5,6,7,8)
    val kk = (d,c)





   println(11/4)
    /*
    println(args(0))

  val filepath = "E:\\scala-workspace\\Spark\\data\\1002.txt"

  val file = Source.fromFile(filepath)
  val ll = file.getLines()
  //  for(l<-ll)
  //    print(l)

    //val user_profile_rdd = sc.textFile(data_input_user_profile).filter(_.split("\\|").length > 50).map(line => (line.split("\\|")(3),(line.split("\\|")(2), line.split("\\|")(25), line.split("\\|")(26), line.split("\\|")(31), line.split("\\|")(32)))).filter( _._1.toString.nonEmpty)
    val result = ll.map(line=>((line.split("\\|",-1)(3),//手机号
      (line.split("\\|")(2),//usre_id
        line.split("\\|")(25),//在网时长
        line.split("\\|")(26),//出账金额
        line.split("\\|")(31),//用户等级
        line.split("\\|")(32)//沃信用分
        )))).filter( _._1.toString.nonEmpty)

  for(r<-result)
    println(r);
  }

  val a = ("(15560476182,(B7614091332597122,38,,,))","(10000000,(B76140913,38,,,))")
  val b = ("(15560476182,(x,x,,,))","(10000000,(b,b,b,,))")
  //var c =a.
*/
  }
}