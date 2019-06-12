package tianchengDataMining

import BaseUtils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory
import scopt.OptionParser

object Preprocess {
  val logger = LoggerFactory.getLogger(getClass)
  Logger.getLogger("org").setLevel(Level.ERROR)

  def run(params: Params): Unit = {
    val spark = SparkSession
      .builder()
      .appName("preprocess")
      .master("local[2]")
      .getOrCreate()

    val sc = spark.sparkContext
    remove_existed_preprocess_dirs(params)
    logger.info("Step_0: 数据预处理，提取数据基本列")

    // 原始操作记录
    val operation_log = spark.read.format("csv").option("header", "true")
      .load(params.operation_input_path).na.fill("").rdd
    operation_log.map(x => x.mkString("|")).saveAsTextFile(params.operation_log_output_path)

    // 精简的操作记录
    logger.info("保存精简的操作记录数据...")
    val op_base_cols = split_basic_operation_cols(sc, params.operation_log_output_path)
    op_base_cols.map(BaseUtils.tupleToString(_, "|"))
      .saveAsTextFile(params.op_base_cols_output_path)

    // 原始的交易记录
    val transaction_log = spark.read.format("csv").option("header", "true")
      .load(params.transaction_input_path).na.fill("").rdd
    transaction_log.map(x => x.mkString("|")).saveAsTextFile(params.transaction_log_output_path)

    // 精简的交易记录
    logger.info("保存精简的交易记录数据...")
    val trans_base_cols = split_basic_trans_cols(sc, params.transaction_log_output_path)
    trans_base_cols.map(BaseUtils.tupleToString(_, "|"))
      .saveAsTextFile(params.trans_base_cols_output_path)

    // 标签记录
    logger.info("保存用户标签记录数据...")
    val tag_file = BaseUtils.exists(params.tag_input_path, params.tag_input_path)
    if (tag_file) {
      val tag_log = spark.read.format("csv").option("header", "true")
        .load(params.tag_input_path).na.fill("").rdd
      tag_log.map(x => x.mkString("|")).saveAsTextFile(params.tag_log_output_path)
    } else {
      val tag_log = BaseRDD.get_all_users(sc, params.op_base_cols_output_path,
        params.trans_base_cols_output_path).map {
        case uid => (uid, 0.0)
      }
      tag_log.map(BaseUtils.tupleToString(_, "|")).saveAsTextFile(params.tag_log_output_path)
    }

  }

  // operation_log: (uid, op_day, op_mode, op_time)
  def split_basic_operation_cols(sc: SparkContext,
                               operation_output_path: String): RDD[(String, String, String, String)] = {

    val op_base_cols = sc.textFile(operation_output_path).map {
      line =>
        val strArr = line.split("\\|", -1)
        val uid = strArr(TianChengConstant.OPERATION_UID)
        val op_day = strArr(TianChengConstant.OPERATION_DAY)
        val op_mode = strArr(TianChengConstant.OPERATION_MODE)
        val op_time = strArr(TianChengConstant.OPERATION_TIME)
        (uid, op_day, op_mode, op_time)
    }.distinct()
    op_base_cols
  }

  // transaction_log :(uid, trans_channel, trans_day, trans_amt, trans_time, trans_type1, trans_type2)
  def split_basic_trans_cols(sc: SparkContext, transaction_output_path: String): RDD[(String,
    String, String, Double, String, String, String)] = {

    val trans_base_cols = sc.textFile(transaction_output_path).map {
      line =>
        val strArr = line.split("\\|", -1)
        val uid = strArr(TianChengConstant.TRANSACTION_UID)
        val trans_channel = strArr(TianChengConstant.TRANSACTION_CHANNEL)
        val trans_day = strArr(TianChengConstant.TRANSACTION_DAY)
        val trans_amt = strArr(TianChengConstant.TRANSACTION_TRANS_AMT).toDouble
        val trans_time = strArr(TianChengConstant.TRANSACTION_TIME)
        val trans_type1 = strArr(TianChengConstant.TRANSACTION_TRANS_TYPE1)
        val trans_type2 = strArr(TianChengConstant.TRANSACTION_TRANS_TYPE2)
        (uid, trans_channel, trans_day, trans_amt, trans_time, trans_type1, trans_type2)
    }.distinct()
    trans_base_cols
  }

  def remove_existed_preprocess_dirs(params: Params): Unit = {
    BaseUtils.remove_existed_dir(params.operation_log_output_path)
    BaseUtils.remove_existed_dir(params.transaction_log_output_path)
    BaseUtils.remove_existed_dir(params.tag_log_output_path)
    BaseUtils.remove_existed_dir(params.op_base_cols_output_path)
    BaseUtils.remove_existed_dir(params.trans_base_cols_output_path)
  }

  def main(args: Array[String]): Unit ={

    val defaultParams = Params()
    val parser = new OptionParser[Params]("preprocess"){
      opt[String]("operation_input_path")
        .action((x, c) => (c.copy(operation_input_path = x)))
      opt[String]("transaction_input_path")
        .action((x, c) => (c.copy(transaction_input_path = x)))
      opt[String]("tag_input_path")
        .action((x, c) => (c.copy(tag_input_path = x)))
      opt[String]("operation_log_output_path")
        .action((x, c) => (c.copy(operation_log_output_path = x)))
      opt[String]("transaction_log_output_path")
        .action((x, c) => (c.copy(transaction_log_output_path = x)))
      opt[String]("tag_log_output_path")
        .action((x, c) => (c.copy(tag_log_output_path = x)))
      opt[String]("op_base_cols_output_path")
        .action((x, c) => (c.copy(op_base_cols_output_path = x)))
      opt[String]("trans_base_cols_output_path")
        .action((x, c) => (c.copy(trans_base_cols_output_path = x)))

      checkConfig(params => success)
    }

    parser.parse(args, defaultParams).map{
      params => run(params)
    }.getOrElse{
      System.exit(1)
    }

  }

  case class Params(
                operation_input_path: String = "/home/chroot/test_data/tiancheng_data/origin_data/train_data/operation_train_new.csv",
                transaction_input_path: String = "/home/chroot/test_data/tiancheng_data/origin_data/train_data/transaction_train_new.csv",
                tag_input_path: String = "/home/chroot/test_data/tiancheng_data/origin_data/train_data/tag_train_new.csv",
                operation_log_output_path: String = "/home/chroot/test_data/tiancheng_data/step_0/operation_train",
                transaction_log_output_path: String = "/home/chroot/test_data/tiancheng_data/step_0/transaction_train",
                tag_log_output_path: String = "/home/chroot/test_data/tiancheng_data/step_0/tag_train",
                op_base_cols_output_path: String = "/home/chroot/test_data/tiancheng_data/step_0/op_base_cols",
                trans_base_cols_output_path: String = "/home/chroot/test_data/tiancheng_data/step_0/trans_base_cols"
              )
}
