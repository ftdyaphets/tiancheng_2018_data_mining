package tianchengDataMining

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory
import scopt.OptionParser

import scala.collection.mutable.ListBuffer

object ActionTypeFeature {
  val logger = LoggerFactory.getLogger(getClass)
  Logger.getLogger("org").setLevel(Level.ERROR)

  def run(params: Params): Unit = {
    val spark = SparkSession
      .builder()
      .appName("action_type_feature")
      .master("local[2]")
      .getOrCreate()

    val sc = spark.sparkContext
    remove_action_type_dirs(params)
    logger.info("Step_1_3: 用户行为类型特征提取")

    val op_base_cols = BaseRDD.get_basic_operation_cols(sc, params.op_base_cols_input_path)
    val trans_base_cols = BaseRDD.get_basic_trans_cols(sc, params.trans_base_cols_input_path)

    val op_type_count = get_op_type_count(op_base_cols)
    //BaseUtils.print_rdd(op_type_count.map(_.toString), "op_type_count")
    op_type_count.coalesce(1).map(BaseUtils.tupleToString(_, "|"))
      .saveAsTextFile(params.op_type_count_output_path)

    val trans_type_count = get_trans_type_count(trans_base_cols)
    //BaseUtils.print_rdd(trans_type_count.map(_.toString), "trans_type_count")
    trans_type_count.coalesce(1).map(BaseUtils.tupleToString(_, "|"))
      .saveAsTextFile(params.trans_type_count_output_path)

    val op_type2Index = get_op_type2Index(op_type_count)
    op_type2Index.coalesce(1).map(BaseUtils.tupleToString(_, "|"))
      .saveAsTextFile(params.op_type2Index_output_path)

    val trans_type2Index = get_trans_type2Index(trans_type_count)
    trans_type2Index.coalesce(1).map(BaseUtils.tupleToString(_, "|"))
      .saveAsTextFile(params.trans_type2Index_output_path)

    logger.info("用户操作次数top3类型...")
    val top3_op_types = get_op_type_top3(op_base_cols, op_type2Index)
    //BaseUtils.print_rdd(top3_op_types.map(_.toString), "top3_op_types")
    top3_op_types.map(BaseUtils.tupleToString(_, "|"))
      .saveAsTextFile(params.top3_op_types_output_path)

    logger.info("用户交易次数top3类型...")
    val top3_trans_types = get_trans_type_top3(trans_base_cols, trans_type2Index)
    //BaseUtils.print_rdd(top3_trans_types.map(_.toString), "top3_trans_types")
    top3_trans_types.map(BaseUtils.tupleToString(_, "|"))
      .saveAsTextFile(params.top3_trans_types_output_path)
  }

  // 操作类型数量统计
  def get_op_type_count(op_base_cols: RDD[(String, String, String, String)]): RDD[(String, Int)] = {
    val op_type = op_base_cols.map {
      case (uid, op_day, op_type, op_time) => (op_type, 1)
    }.reduceByKey(_ + _).sortBy(-_._2)
    op_type
  }

  // 交易类型数量统计
  def get_trans_type_count(trans_base_cols: RDD[(String, String, String, Double, String, String, String)]): RDD[(String, Int)] = {
    val trans_type = trans_base_cols.map {
      case (uid, trans_channel, trans_day, trans_amt, trans_time, trans_type1, trans_type2) =>
        (trans_type1, 1)
    }.reduceByKey(_ + _).sortBy(-_._2)
    trans_type
  }

  // 用户操作次数top3类型
  def get_op_type_top3(op_base_cols: RDD[(String, String, String, String)],
                       op_type2Index: RDD[(String, String)]): RDD[(String, String)] = {
    // 基本列中的字符串类型转换成数字，便于输入模型
    val op_base_replace_type = op_base_cols.map {
      case (uid, op_day, op_type, op_time) => (op_type, (uid, op_day, op_time))
    }.join(op_type2Index).map {
      case (op_type, ((uid, op_day, op_time), index)) => (uid, op_day, index, op_time)
    }
    val top3_op_types = op_base_replace_type.map {
      case (uid, op_day, op_type, op_time) => ((uid, op_type), 1)
    }.reduceByKey(_ + _).map {
      case ((uid, op_type), count) => (uid, (op_type, count))
    }.groupByKey().mapValues {
      x =>
        val top3 = x.toList.sortBy(-_._2).take(3).unzip
        var top3_op_type = new ListBuffer[String]
        for (i <- 0 until 3) {
          if (i < top3._1.length) {
            top3_op_type.append(top3._1(i).toString)
          } else {
            top3_op_type.append("0")
          }

        }
        top3_op_type.mkString("|")
    }
    top3_op_types
  }

  // 用户交易次数top3类型
  def get_trans_type_top3(trans_base_cols: RDD[(String, String, String, Double, String, String, String)],
                          trans_type2Index: RDD[(String, String)]): RDD[(String, String)] = {
    // 基本列中的字符串类型转换成数字，便于输入模型
    val trans_base_replace_type = trans_base_cols.map {
      case (uid, trans_channel, trans_day, trans_amt, trans_time, trans_type1, trans_type2) =>
        (trans_type1, (uid, trans_channel, trans_day, trans_amt, trans_time, trans_type2))
    }.join(trans_type2Index).map {
      case (trans_type1, ((uid, trans_channel, trans_day, trans_amt, trans_time, trans_type2), index)) =>
        (uid, trans_channel, trans_day, trans_amt, trans_time, index, trans_type2)
    }
    val top3_trans_types = trans_base_replace_type.map {
      case (uid, trans_channel, trans_day, trans_amt, trans_time, trans_type1, trans_type2) =>
        ((uid, trans_type1), 1)
    }.reduceByKey(_ + _).map {
      case ((uid, trans_type1), count) => (uid, (trans_type1, count))
    }.groupByKey().mapValues {
      x =>
        val top3 = x.toList.sortBy(-_._2).take(3).unzip
        var top3_trans_types = new ListBuffer[String]
        for (i <- 0 until 3) {
          if (i < top3._1.length) {
            top3_trans_types.append(top3._1(i).toString)
          } else {
            top3_trans_types.append("0")
          }

        }
        top3_trans_types.mkString("|")
    }
    top3_trans_types
  }

  // 操作类型（String）转换成数字
  def get_op_type2Index(op_type_count: RDD[(String, Int)]): RDD[(String, String)] = {
    val op_type2Index = op_type_count.map {
      case (op_type, count) => op_type
    }.zipWithIndex().map(x => (x._1, generate_type_id("0", x._2 + 1)))
    BaseUtils.print_rdd(op_type2Index.map(_.toString), "op_type2Index")
    op_type2Index
  }

  // 交易类型（String）转换成数字
  def get_trans_type2Index(trans_type_count: RDD[(String, Int)]): RDD[(String, String)] = {
    val trans_type2Index = trans_type_count.map {
      case (trans_type, count) => trans_type
    }.zipWithIndex().map(x => (x._1, generate_type_id("1", x._2 + 1)))
    BaseUtils.print_rdd(trans_type2Index.map(_.toString), "trans_type2Index")
    trans_type2Index
  }

  // 统一生成三位的类型id，第一位"0”为操作类型，“1”为交易类型，后两位为具体操作下标
  def generate_type_id(type_num: String, index: Long): String = {
    val index_str = if (index < 10) "0" + index else index
    val type_id = type_num + index_str
    type_id
  }

  def remove_action_type_dirs(params: Params): Unit = {
    BaseUtils.remove_existed_dir(params.op_type_count_output_path)
    BaseUtils.remove_existed_dir(params.trans_type_count_output_path)
    BaseUtils.remove_existed_dir(params.op_type2Index_output_path)
    BaseUtils.remove_existed_dir(params.trans_type2Index_output_path)
    BaseUtils.remove_existed_dir(params.top3_op_types_output_path)
    BaseUtils.remove_existed_dir(params.top3_trans_types_output_path)
  }

  def main(args: Array[String]): Unit ={

    val defaultParams = Params()
    val parser = new OptionParser[Params]("action_type_feature"){
      opt[String]("op_base_cols_input_path")
        .action((x, c) => (c.copy(op_base_cols_input_path = x)))
      opt[String]("trans_base_cols_input_path")
        .action((x, c) => (c.copy(trans_base_cols_input_path = x)))
      opt[String]("op_type_count_output_path")
        .action((x, c) => (c.copy(op_type_count_output_path = x)))
      opt[String]("trans_type_count_output_path")
        .action((x, c) => (c.copy(trans_type_count_output_path = x)))
      opt[String]("op_type2Index_output_path")
        .action((x, c) => (c.copy(op_type2Index_output_path = x)))
      opt[String]("trans_type2Index_output_path")
        .action((x, c) => (c.copy(trans_type2Index_output_path = x)))
      opt[String]("top3_op_types_output_path")
        .action((x, c) => (c.copy(top3_op_types_output_path = x)))
      opt[String]("top3_trans_types_output_path")
        .action((x, c) => (c.copy(top3_trans_types_output_path = x)))

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
                     op_type_count_output_path: String = "/home/chroot/test_data/tiancheng_data/step_1/op_type_count",
                     trans_type_count_output_path: String = "/home/chroot/test_data/tiancheng_data/step_1/trans_type_count",
                     op_type2Index_output_path: String = "/home/chroot/test_data/tiancheng_data/step_1/op_type2Index",
                     trans_type2Index_output_path: String = "/home/chroot/test_data/tiancheng_data/step_1/trans_type2Index",
                     top3_op_types_output_path: String = "/home/chroot/test_data/tiancheng_data/step_1/top3_op_types",
                     top3_trans_types_output_path: String = "/home/chroot/test_data/tiancheng_data/step_1/top3_trans_types"
                   )
}
