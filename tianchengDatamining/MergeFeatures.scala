package tianchengDataMining

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory
import scopt.OptionParser
import org.apache.spark.rdd.RDD

object MergeFeatures {
  val logger = LoggerFactory.getLogger(getClass)
  Logger.getLogger("org").setLevel(Level.ERROR)

  def run(params: Params): Unit = {
    val spark = SparkSession
      .builder()
      .appName("merge_features")
      .master("local[2]")
      .getOrCreate()

    val sc = spark.sparkContext
    remove_merged_features_dirs(params)
    logger.info("Step_1_4: 融合特征数据")

    val merged_features = merge_features(sc, params)
    //BaseUtils.print_rdd(merged_features.map(_.toString), "merged_features")

    merged_features.saveAsTextFile(params.merged_features_output_path)
  }

  // 融合特征数据
  def merge_features(sc: SparkContext, params: Params): RDD[String] = {

    val tag_log = sc.textFile(params.tag_log_input_path).map {
      line =>
        val strArr = line.split("\\|", -1)
        val uid = strArr(0)
        val tag = strArr(1)
        (uid, tag.toDouble)
    }
    // (uid, min_amt, max_amt, midle_amt, avg_amt, std_amt)
    val user_amt_sta = split_uid_and_features(sc.textFile(params.user_amt_sta_input_path))
    //BaseUtils.print_rdd(user_amt_sta.map(_.toString), "user_amt_sta")

    // (uid, amt_count, thousand_count_rate, ten_thousand_count_rate, hundred_thousand_count_rate)
    val user_amt_range = split_uid_and_features(sc.textFile(params.user_amt_range_input_path))
    //BaseUtils.print_rdd(user_amt_range.map(_.toString), "user_amt_range")

    // (uid, op_days_range_count, op_early_days_rate, op_middle_days_rate, op_end_days_rate,
    // trans_days_range_count, trans_early_days_rate, trans_middle_days_rate, trans_end_days_rate)
    val action_days_range = split_uid_and_features(sc.textFile(params.action_days_range_input_path))
    //BaseUtils.print_rdd(action_days_range.map(_.toString), "action_days_range")

    // (uid, op_time_range_count, op_night_rate, op_morning_rate, op_noon_rate, op_afternoon_rate, op_evening_rate,
    // trans_time_range_count, trans_night_rate, trans_morning_rate, trans_noon_rate, trans_afternoon_rate, trans_evening_rate)
    val action_time_range = split_uid_and_features(sc.textFile(params.action_time_range_input_path))
    //BaseUtils.print_rdd(action_time_range.map(_.toString), "action_time_range")

    // (uid, max_1_op_type, max_2_op_type, max_3_op_type))
    val top3_op_types = split_uid_and_features(sc.textFile(params.top3_op_types_input_path))
    BaseUtils.print_rdd(top3_op_types.map(_.toString), "top3_op_types")

    // (uid, max_1_trans_type, max_2_trans_type, max_3_trans_type))
    val top3_trans_types = split_uid_and_features(sc.textFile(params.top3_trans_types_input_path))
    BaseUtils.print_rdd(top3_trans_types.map(_.toString), "top3_trans_types")

    val related_action_feature = split_uid_and_features(sc.textFile(params.related_action_feature_input_path))
    BaseUtils.print_rdd(related_action_feature.map(_.toString), "related_action_feature")

    val tag_join_amt = tag_log.join(user_amt_sta).map {
      case (uid, (tag, amt_sta)) => (uid, tag + "|" + amt_sta)
    }.join(user_amt_range).map {
      case (uid, (tag_amt_sta, amt_range)) => (uid, tag_amt_sta + "|" + amt_range)
    }
    BaseUtils.print_rdd(tag_join_amt.map(_.toString), "tag_join_amt")

    val join_amt_time = tag_join_amt.join(action_days_range).map {
      case (uid, (tag_amt, days_range)) => (uid, tag_amt + "|" + days_range)
    }.join(action_time_range).map {
      case (uid, (tag_amt_days, time_range)) => (uid, tag_amt_days + "|" + time_range)
    }
    BaseUtils.print_rdd(join_amt_time.map(_.toString), "join_amt_time")

    val join_amt_time_action_type = join_amt_time.join(top3_op_types).map {
      case (uid, (tag_amt_time, op_type)) =>(uid, tag_amt_time + "|" + op_type)
    }.join(top3_trans_types).map {
      case (uid, (tag_amt_time_op_type, trans_type)) =>(uid, tag_amt_time_op_type + "|" + trans_type)
    }
    BaseUtils.print_rdd(join_amt_time_action_type.map(_.toString), "join_amt_time_action_type")

    val join_amt_time_action_feature = join_amt_time_action_type.join(related_action_feature).map {
      case (uid, (tag_amt_time_action_type, related_action)) =>
        (uid, tag_amt_time_action_type + "|" + related_action)
    }
    BaseUtils.print_rdd(join_amt_time_action_feature.map(_.toString), "join_amt_time_action_feature")

    join_amt_time_action_feature.map(BaseUtils.tupleToString(_, "|"))
  }


  def split_uid_and_features(rdd: RDD[String]): RDD[(String, String)] = {
    val splited_rdd = rdd.map {
      line =>
        val first_separator_index = line.indexOf("|")
        val uid = line.substring(0, first_separator_index)
        val features  = line.substring(first_separator_index + 1)
        (uid, features)
    }
    splited_rdd
  }

  def remove_merged_features_dirs(params: Params): Unit = {
    BaseUtils.remove_existed_dir(params.merged_features_output_path)
  }

  def main(args: Array[String]): Unit ={

    val defaultParams = Params()
    val parser = new OptionParser[Params]("preprocess"){
      opt[String]("tag_log_input_path")
        .action((x, c) => (c.copy(tag_log_input_path = x)))
      opt[String]("action_days_range_input_path")
        .action((x, c) => (c.copy(action_days_range_input_path = x)))
      opt[String]("action_time_range_input_path")
        .action((x, c) => (c.copy(action_time_range_input_path = x)))
      opt[String]("top3_op_types_input_path")
        .action((x, c) => (c.copy(top3_op_types_input_path = x)))
      opt[String]("top3_trans_types_input_path")
        .action((x, c) => (c.copy(top3_trans_types_input_path = x)))
      opt[String]("user_amt_sta_input_path")
        .action((x, c) => (c.copy(user_amt_sta_input_path = x)))
      opt[String]("user_amt_range_input_path")
        .action((x, c) => (c.copy(user_amt_range_input_path = x)))
      opt[String]("related_action_feature_input_path")
        .action((x, c) => (c.copy(related_action_feature_input_path = x)))
      opt[String]("merged_features_output_path")
        .action((x, c) => (c.copy(merged_features_output_path = x)))

      checkConfig(params => success)
    }

    parser.parse(args, defaultParams).map{
      params => run(params)
    }.getOrElse{
      System.exit(1)
    }

  }

  case class Params(
                     tag_log_input_path: String = "file:///home/chroot/test_data/tiancheng_data/step_0/tag_train",
                     action_days_range_input_path: String = "/home/chroot/test_data/tiancheng_data/step_1/action_days_range",
                     action_time_range_input_path: String = "/home/chroot/test_data/tiancheng_data/step_1/action_time_range",
                     top3_op_types_input_path: String = "/home/chroot/test_data/tiancheng_data/step_1/top3_op_types",
                     top3_trans_types_input_path: String = "/home/chroot/test_data/tiancheng_data/step_1/top3_trans_types",
                     user_amt_sta_input_path: String = "/home/chroot/test_data/tiancheng_data/step_1/user_amt_sta",
                     user_amt_range_input_path: String = "/home/chroot/test_data/tiancheng_data/step_1/user_amt_range",
                     related_action_feature_input_path: String = "/home/chroot/test_data/tiancheng_data/step_2/related_action_feature",
                     merged_features_output_path: String = "/home/chroot/test_data/tiancheng_data/step_2/merged_features"
                   )
}
