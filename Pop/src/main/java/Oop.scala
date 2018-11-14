import org.apache.spark.{SparkConf, SparkContext}

object Oop {


    def main(args: Array[String]): Unit = {
      println("111")
      //第一步：创建sparkConf
      val conf = new SparkConf().setMaster("local[*]").setAppName("wordcount")
      //第二步：创建sparkContext
      val sc = new SparkContext(conf)
      //第三步：通过sparkcontext读取数据，生成rdd
      //4:表示分区尽可能与CPU合数相同的数
      val rdd=sc.textFile("D:\\test\\1.txt",2)
      //path:代表文件存储路径 minPartitions:最小分区数
      //分区是spark进行并行计算的基础

//      println(rdd)
//      rdd.collect().foreach(println)

      rdd.flatMap(line=>line.split(" ")).groupBy(x=>x)
        .map(kv=>(kv._1,kv._2.size))
        .collect().foreach(println)

      //第四步：把rdd作为一个集合，进行各种运算

      //    rdd.groupBy(line=>line.split("\t")(0))
      //      .collect().foreach(println)
      //
          Thread.sleep(10000000)
    }




}
