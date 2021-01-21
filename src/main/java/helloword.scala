import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object helloword{
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    /**
      * wordCount
      */

//    val conf = new SparkConf().setMaster("local").setAppName("wordCount")
//    val sc = new SparkContext(conf)
//
//    val rdd = sc.textFile("C:\\Users\\Administrator\\Desktop\\data\\The_Man_of_Property.txt")
//    rdd.flatMap{x =>
//      x.split(" ")
//    }.map((_,1)).reduceByKey(_+_).saveAsTextFile("C:\\Users\\Administrator\\Desktop\\data\\1.txt")



    val conf =new SparkConf().setMaster("local").setAppName("test")
    val sc =new SparkContext(conf)

    val file = sc.textFile("hdfs://master:9000/test.txt")
//    val num = file.map{ x =>
//      var ss =x.split(" ")
//      var a=ss(0)
//      var b=ss(1)
//      var c=ss(2)
//      var d=ss(3)
//      var e=ss(4)
//      var f=ss(5)
      /**
        * 2. 一共有多少个小于20岁的人参加考试？
        * (((c,a+"\t"+b+"\t"+c),d+"\t"+e+"\t"+f))
        * }.filter(_._1._1.toInt<20).groupByKey().count()
        * print (num)
        */

      /**
        *  5. 一共有多个男生参加考试？
        * (((d,a+"\t"+b+"\t"+c),d+"\t"+e+"\t"+f))
        * }.filter(_._1._1=="男").groupByKey().count()
        * print (num)
        */


      /**
        * 7.12班有多少人参加考试？
        * (((a,a+"\t"+b+"\t"+c),d+"\t"+e+"\t"+f))
        * }.filter(_._1._1.toInt==12).groupByKey().count()
        * println(num)
        */
//      (((a,a+"\t"+b+"\t"+c),d+"\t"+e+"\t"+f))
//       }.filter(_._1._1.toInt==12).groupByKey().count()
//    println(num)

    /**
      * 语文科目的平均成绩是多少？
      * var score = file.map(_.split(" ")).filter(_(4)=="chinese").map(_(5).toInt).mean()
      */

    /**
      * 每个人平均成绩是多少？
      * file.map(_.split(" ")).map(x=>(x(1),x(5).toInt)).groupByKey().map( x =>(x._1,x._2.sum / x._2.size)).foreach(println(_))
      */

    /**
      * 12班平均成绩是多少？
      * var score =file.map(_.split(" ")).filter(_(0).toInt==12).map(_(5).toInt).mean()
      */

    /**
      * 每个班的平均成绩
      * file.map(_.split(" ")).map(x=>(x(0),x(5).toInt)).groupByKey().map(x=>(x._1+"班",x._2.sum/x._2.size)).foreach(println(_))
      */

    /**
      * 13班男生平均总成绩是多少？
      * var score = file.map(_.split(" ")).filter( x =>(x(0).toInt==13 && x(3)=="男"))
      * .map(x=>(x(1),x(5).toInt)).groupByKey().map(x =>(x._1,x._2.sum)).map(_._2).mean()
      */


    /**
      * 全校语文成绩最高分是多少？
      * var score = file.map(_.split(" ")).filter( x =>(x(4)=="chinese")).map(x =>x(5).toInt).max()
      */

    /**
      * 全校单科第一
      * file.map(_.split(" ")).map(x =>(x(1),x(4),x(5))).sortBy(x => (x._2,x._3)).groupBy(_._2).map{ x =>
      * val is_list = x._2
      * val is_arr = is_list.toArray
      * var i_us_arr = new ArrayBuffer[(String,String,Int)]
      * val data = is_arr(is_arr.length-1)
      * data
      * }.foreach(println(_))
      */

    /**
      * 12班语文成绩最低分是多少？50
      * val score = file.map(_.split(" ")).filter(x =>(x(0).toInt==12 && x(4) =="chinese")).map(_(5).toInt).min()
      */


    /**
      * 总成绩大于150分的12班的女生有几个？1
      * file.map(_.split(" ")).filter(x =>(x(0).toInt==12 && x(3) =="女"))
      * .map(x=>(x(1),x(5).toInt)).groupByKey().map(x=>(x._1,x._2.sum))
      * .filter(_._2>150).count()
      * 总成绩大于150分的12班的女生都有谁。
      * .foreach(println(_))
      */



    /**
      * 总成绩大于150分，且数学大于等于70，且年龄大于等于19岁的学生的平均成绩是多少？
      */

    /**
      * 1、过滤出总分大于150的，并求出平均成绩
      */


    //(13,李逵,男 , 60)


    //println(complex1)

  }
}