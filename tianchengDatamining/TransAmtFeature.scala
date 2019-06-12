package tianchengDataMining

import BaseUtils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory
import scopt.OptionParser

object TransAmtFeature {
  val logger = LoggerFactory.getLogger(getClass)
  Logger.getLogger("org").setLevel(Level.ERROR)

  def run(params: Params): Unit = {
    val spark = SparkSession
      .builder()
      .appName("trans_amt_feature")
      .master("local[2]")
      .getOrCreate()

    val sc = spark.sparkContext
    remove_trans_amt_dirs(params)
    logger.info("Step_1_1: 用户交易金额特征提取")

    val trans_base_cols = BaseRDD.get_basic_trans_cols(sc, params.trans_base_cols_input_path)

    logger.info("用户交易金额统计值...")
    val user_amt_sta = get_user_amt_sta(trans_base_cols)
    //BaseUtils.print_rdd(user_amt_sta.map(_.toString), "user_amt_sta")
    user_amt_sta.map(BaseUtils.tupleToString(_, "|")).saveAsTextFile(params.user_amt_sta_output_path)

    val trans_amt_range = get_trans_amt_range(trans_base_cols)
    //BaseUtils.print_rdd(trans_amt_range.map(_.toString), "trans_amt_range")
    trans_amt_range.coalesce(1).saveAsTextFile(params.trans_amt_range_output_path)

    logger.info("用户交易金额分布...")
    val user_amt_range = get_user_amt_range(trans_base_cols)
    //BaseUtils.print_rdd(user_amt_range.map(_.toString), "user_amt_range")
    user_amt_range.saveAsTextFile(params.user_amt_range_output_path)
  }

  // 获取用户交易额最小值、最大值、中位数、平均值，标准差
  def get_user_amt_sta(trans_base_cols: RDD[(String, String, String, Double, String, String, String)]): RDD[(String, String)] = {
    val user_amt_sta = trans_base_cols.map {
      case (uid, trans_channel, trans_day, trans_amt, trans_time, trans_type1, trans_type2) =>
        (uid, trans_amt)
    }.groupByKey().mapValues {
      x =>
        val amt_array = x.toArray
        BaseUtils.sta_Count(amt_array)
    }
    user_amt_sta
  }

  // 获取所有交易额范围分布
  def get_trans_amt_range(trans_base_cols: RDD[(String, String, String, Double, String, String, String)]): RDD[String] = {
    val trans_amt_count_range = trans_base_cols.map {
      case (uid, trans_channel, trans_day, trans_amt, trans_time, trans_type1, trans_type2) =>
        (1, trans_amt)
    }.groupByKey().mapValues {
      x =>
        val amt_count = x.size
        val amt_min = x.min
        val amt_max = x.max
        val hundred_count_rate = (x.count(amt => amt <= 100)*100.0 / amt_count).formatted("%.2f")
        val thousand_count_rate = (x.count(amt => amt > 100 && amt < 1000)*100.0 / amt_count).formatted("%.2f")
        val ten_thousand_count_rate = (x.count(amt => amt > 1000 && amt < 10000)*100.0 / amt_count).formatted("%.2f")
        val hundred_thousand_count_rate = (x.count(amt => amt > 10000)*100.0 / amt_count).formatted("%.2f")
        (amt_count, amt_min, amt_max, hundred_count_rate, thousand_count_rate,
          ten_thousand_count_rate, hundred_thousand_count_rate)
    }.map {
      case (uid, (amt_count, amt_min, amt_max, hundred_count_rate, thousand_count_rate,
      ten_thousand_count_rate, hundred_thousand_count_rate)) =>
        (amt_count, amt_min, amt_max, hundred_count_rate, thousand_count_rate,
          ten_thousand_count_rate, hundred_thousand_count_rate)
    }
    trans_amt_count_range.map(BaseUtils.tupleToString(_, "|"))
  }

  // 获取用户交易额范围分布
  def get_user_amt_range(trans_base_cols: RDD[(String, String, String, Double, String, String, String)]): RDD[String] = {
    val user_amt_count_range = trans_base_cols.map {
      case (uid, trans_channel, trans_day, trans_amt, trans_time, trans_type1, trans_type2) =>
        (uid, trans_amt)
    }.groupByKey().mapValues {
      x =>
        val amt_count = x.size
        val thousand_count_rate = (x.count(amt => amt > 100 && amt < 1000)*100.0 / amt_count).formatted("%.2f")
        val ten_thousand_count_rate = (x.count(amt => amt > 1000 && amt < 10000)*100.0 / amt_count).formatted("%.2f")
        val hundred_thousand_count_rate = (x.count(amt => amt > 10000)*100.0 / amt_count).formatted("%.2f")
        (amt_count, thousand_count_rate, ten_thousand_count_rate, hundred_thousand_count_rate)
    }.map {
      case (uid, (amt_count, thousand_count_rate, ten_thousand_count_rate,
      hundred_thousand_count_rate)) =>
        (uid, amt_count, thousand_count_rate, ten_thousand_count_rate,
          hundred_thousand_count_rate)
    }.sortBy(_._2, false)
    user_amt_count_range.map(BaseUtils.tupleToString(_, "|"))
  }

  def remove_trans_amt_dirs(params: Params): Unit = {
    BaseUtils.remove_existed_dir(params.user_amt_sta_output_path)
    BaseUtils.remove_existed_dir(params.trans_amt_range_output_path)
    BaseUtils.remove_existed_dir(params.user_amt_range_output_path)
  }

  def main(args: Array[String]): Unit ={

    val defaultParams = Params()
    val parser = new OptionParser[Params]("preprocess"){
      opt[String]("trans_base_cols_input_path")
        .action((x, c) => (c.copy(trans_base_cols_input_path = x)))
      opt[String]("user_amt_sta_output_path")
        .action((x, c) => (c.copy(user_amt_sta_output_path = x)))
      opt[String]("trans_amt_range_output_path")
        .action((x, c) => (c.copy(trans_amt_range_output_path = x)))
      opt[String]("user_amt_range_output_path")
        .action((x, c) => (c.copy(user_amt_range_output_path = x)))

      checkConfig(params => success)
    }

    parser.parse(args, defaultParams).map{
      params => run(params)
    }.getOrElse{
      System.exit(1)
    }

  }

  case class Params(
                     trans_base_cols_input_path: String = "/home/chroot/test_data/tiancheng_data/step_0/trans_base_cols",
                     user_amt_sta_output_path: String = "/home/chroot/test_data/tiancheng_data/step_1/user_amt_sta",
                     trans_amt_range_output_path: String = "/home/chroot/test_data/tiancheng_data/step_1/trans_amt_range",
                     user_amt_range_output_path: String = "/home/chroot/test_data/tiancheng_data/step_1/user_amt_range"
                   )
}
