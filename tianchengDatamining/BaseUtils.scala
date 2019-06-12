package tianchengDataMining

import org.apache.spark.rdd.RDD

object BaseUtils {

  def remove_existed_dir(path: String): Unit = {
    removeDir(path, path)
  }

  def print_rdd(rdd: RDD[String], name: String): Unit = {
    println("##############################################")
    println(s"$name count: " + rdd.count)
    rdd.take(20).foreach(println)
  }
  
  def mean(arr: Array[Double]): Double = {
    if (arr.isEmpty) 0
    else arr.sum / arr.length.toDouble
  }

  def media(arr: Array[Double]): Double = {
    val sorted = arr.sorted
    arr.length match {
      case len if len == 0 => 0
      case len if len % 2 == 0 => (sorted(len / 2 - 1) + sorted(len / 2)) / 2.0
      case len => sorted(len / 2)
    }
  }
  
  //排序，求出最小值、最大值、中位数、平均值，标准差
  def sta_Count(arr: Array[Double]) = {
    val splitStr = "|"
    //平均值
    val avg = mean(arr)
    //标准差
    val std_num = arr.map(x => math.pow(x - avg, 2)).sum / arr.size
    //最大值
    val max_num = arr.max
    //最小值
    val min_num = arr.min
    //中位数
    var midleNum = media(arr)
    List(min_num, max_num, midleNum, avg, std_num).mkString(splitStr)
  }
  
  def tupleToString(tuple: Product, split: String): String = {
    var line = ListBuffer[(String)]()
    tuple.productIterator.foreach{ i => line += i.toString }
    line.mkString(split)
  }
  
  def removeDir(dfsURI: String, dir: String): Unit = {
    val hadoopConf = new org.apache.hadoop.conf.Configuration()
    val hdfs = if ("".equals(dfsURI)) FileSystem.get(hadoopConf) else FileSystem.get(new java.net.URI(dfsURI), hadoopConf)
    // Delete the existing path, ignore any exceptions thrown if the path doesn't exist
    try {
      hdfs.delete(new org.apache.hadoop.fs.Path(dir), true)
    } catch {
      case _: Throwable => {}
    }
  }

}
