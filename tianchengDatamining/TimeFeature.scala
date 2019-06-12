package tianchengDataMining

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory
import scopt.OptionParser

object TimeFeature {

  val logger = LoggerFactory.getLogger(getClass)
  Logger.getLogger("org").setLevel(Level.ERROR)

  def run(params: Params): Unit = {
    val spark = SparkSession
      .builder()
      .appName("time_feature")
      .master("local[2]")
      .getOrCreate()

    val sc = spark.sparkContext
    remove_time_dirs(params)
    logger.info("Step_1_2: 时间特征提取")

    val op_base_cols = BaseRDD.get_basic_operation_cols(sc, params.op_base_cols_input_path)
    val trans_base_cols = BaseRDD.get_basic_trans_cols(sc, params.trans_base_cols_input_path)

    logger.info("操作和交易日期分布（上旬、中旬、下旬）...")
    val action_days_range = get_action_days_range(op_base_cols, trans_base_cols)
    action_days_range.saveAsTextFile(params.action_days_range_output_path)

    logger.info("操作和交易时间分布（午夜、上午、中午、下午、晚上 ）...")
    val action_time_range = get_action_time_range(op_base_cols, trans_base_cols)
    action_time_range.saveAsTextFile(params.action_time_range_output_path)
  }

  // 获取操作、交易日期范围分布的融合结果
  def get_action_days_range(op_base_cols: RDD[(String, String, String, String)],
                            trans_base_cols: RDD[(String, String, String, Double, String, String, String)]): RDD[String] = {

    val op_days_range = get_days_range(op_base_cols.map(x => (x._1, x._2)))

    val trans_days_range = get_days_range(trans_base_cols.map(x => (x._1, x._3)))

    val action_days_range = op_days_range.fullOuterJoin(trans_days_range).map {
      case (key, (Some(v1), None)) => (key, v1, ("", "", "", ""))
      case (key, (Some(v1), Some(v2))) => (key, v1, v2)
      case (key, (None, Some(v1))) => (key, ("", "", "", ""), v1)
    }.map {
      case (uid, (op_days_range_count, op_early_days_rate, op_middle_days_rate, op_end_days_rate),
      (trans_days_range_count, trans_early_days_rate, trans_middle_days_rate, trans_end_days_rate)) =>
        (uid, op_days_range_count, op_early_days_rate, op_middle_days_rate, op_end_days_rate,
          trans_days_range_count, trans_early_days_rate, trans_middle_days_rate, trans_end_days_rate)
    }
    action_days_range.map(BaseUtils.tupleToString(_, "|"))
  }

  // 获取操作、交易时间范围分布的融合结果
  def get_action_time_range(op_base_cols: RDD[(String, String, String, String)],
                           trans_base_cols: RDD[(String, String, String, Double, String, String, String)]): RDD[String] = {
    val op_uid_time = op_base_cols.map {
      case (uid, op_day, op_mode, op_time) => (uid, op_time)
    }
    val op_time_range = get_time_range(op_uid_time)

    val trans_uid_time = trans_base_cols.map {
      case (uid, trans_channel, trans_day, trans_amt, trans_time, trans_type1, trans_type2) =>
        (uid, trans_time)
    }
    val trans_time_range = get_time_range(trans_uid_time)

    val action_time_range = op_time_range.fullOuterJoin(trans_time_range).map {
      case (key, (Some(v1), None)) => (key, v1, ("", "", "", "", "", ""))
      case (key, (Some(v1), Some(v2))) => (key, v1, v2)
      case (key, (None, Some(v1))) => (key, ("", "", "", "", "", ""), v1)
    }.map {
      case (uid, (op_time_range_count, op_night_rate, op_morning_rate, op_noon_rate,
      op_afternoon_rate, op_evening_rate), (trans_time_range_count, trans_night_rate,
      trans_morning_rate, trans_noon_rate, trans_afternoon_rate, trans_evening_rate)) =>
        (uid, op_time_range_count, op_night_rate, op_morning_rate, op_noon_rate,
          op_afternoon_rate, op_evening_rate, trans_time_range_count, trans_night_rate,
          trans_morning_rate, trans_noon_rate, trans_afternoon_rate, trans_evening_rate)
    }
    action_time_range.map(BaseUtils.tupleToString(_, "|"))
  }


  // 获取日期范围分布
  def get_days_range(uid_days: RDD[(String, String)]): RDD[(String, (Int,
    String, String, String))] = {

    val days_range = uid_days.map {
      case (uid, day) => (uid, day.toInt)
    }.groupByKey().mapValues {
      x =>
        val days_range_count = x.max - x.min + 1
        val days_count = x.size
        val early_days_count = x.count(x => x <= 10)
        val middle_days_count = x.count(x => x > 10 && x <= 20)
        val end_days_count = x.count(x => x > 20)
        val early_days_rate = (early_days_count * 100.00 / days_count).formatted("%.2f")
        val middle_days_rate = (middle_days_count * 100.00 / days_count).formatted("%.2f")
        val end_days_rate = (end_days_count * 100.00 / days_count).formatted("%.2f")
        (days_range_count, early_days_rate, middle_days_rate, end_days_rate)
    }
    days_range
  }

  // 获取时间范围分布
  def get_time_range(uid_time: RDD[(String, String)]): RDD[(String, (Int, String, String,
    String, String, String))] = {

    val time_range = uid_time.map {
      case (uid, time) => (uid, time.trim().substring(0, 2).toInt)
    }.groupByKey().mapValues {
      x =>
        val time_range_count = x.max - x.min + 1
        val time_count = x.size
        val night_count = x.count(x => x > 0 && x <= 8)
        val morning_count = x.count(x => x > 8 && x <= 12)
        val noon_count = x.count(x => x > 12 && x <= 14)
        val afternoon_count = x.count(x => x > 14 && x <= 18)
        val evening_count = x.count(x => x > 18 && x <= 24)

        val night_rate = (night_count * 100.00 / time_count).formatted("%.2f")
        val morning_rate = (morning_count * 100.00 / time_count).formatted("%.2f")
        val noon_rate = (noon_count * 100.00 / time_count).formatted("%.2f")
        val afternoon_rate = (afternoon_count * 100.00 / time_count).formatted("%.2f")
        val evening_rate = (evening_count * 100.00 / time_count).formatted("%.2f")
        (time_range_count, night_rate, morning_rate, noon_rate, afternoon_rate, evening_rate)
    }
    time_range
  }

  def remove_time_dirs(params: Params): Unit = {
    BaseUtils.remove_existed_dir(params.action_days_range_output_path)
    BaseUtils.remove_existed_dir(params.action_time_range_output_path)
  }

  def main(args: Array[String]): Unit ={

    val defaultParams = Params()
    val parser = new OptionParser[Params]("time_feature"){
      opt[String]("op_base_cols_input_path")
        .action((x, c) => (c.copy(op_base_cols_input_path = x)))
      opt[String]("trans_base_cols_input_path")
        .action((x, c) => (c.copy(trans_base_cols_input_path = x)))
      opt[String]("action_days_range_output_path")
        .action((x, c) => (c.copy(action_days_range_output_path = x)))
      opt[String]("action_time_range_output_path")
        .action((x, c) => (c.copy(action_time_range_output_path = x)))

      checkConfig(params => success)
    }

    parser.parse(args, defaultParams).map{
      params => run(params)
    }.getOrElse{
      System.exit(1)
    }

  }

  case class Params(
                     op_base_cols_input_path: String = "/home/chroot/test_data/tiancheng_data/step_0/op_base_cols",
                     trans_base_cols_input_path: String = "/home/chroot/test_data/tiancheng_data/step_0/trans_base_cols",
                     action_days_range_output_path: String = "/home/chroot/test_data/tiancheng_data/step_1/action_days_range",
                     action_time_range_output_path: String = "/home/chroot/test_data/tiancheng_data/step_1/action_time_range"
                   )

}
