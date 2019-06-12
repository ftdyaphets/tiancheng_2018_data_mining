package tianchengDataMining

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.collection.Map

object BaseRDD {

  // operation_log: (uid, op_day, op_type, op_time)
  def get_basic_operation_cols(sc: SparkContext,
                               op_base_cols_input_path: String): RDD[(String, String, String, String)] = {

    val op_base_cols = sc.textFile(op_base_cols_input_path).map {
      line =>
        val strArr = line.split("\\|", -1)
        val uid = strArr(TianChengConstant.OP_UID)
        val op_day = strArr(TianChengConstant.OP_DAY)
        val op_type = strArr(TianChengConstant.OP_MODE)
        val op_time = strArr(TianChengConstant.OP_TIME)
        (uid, op_day, op_type, op_time)
    }
    op_base_cols
  }

  // transaction_log :(uid, trans_channel, trans_day, trans_amt, trans_time, trans_type1, trans_type2)
  def get_basic_trans_cols(sc: SparkContext, transaction_output_path: String): RDD[(String,
    String, String, Double, String, String, String)] = {

    val trans_base_cols = sc.textFile(transaction_output_path).map {
      line =>
        val strArr = line.split("\\|", -1)
        val uid = strArr(TianChengConstant.TRANS_UID)
        val trans_channel = strArr(TianChengConstant.TRANS_CHANNEL)
        val trans_day = strArr(TianChengConstant.TRANS_DAY)
        val trans_amt = strArr(TianChengConstant.TRANS_AMT).toDouble
        val trans_time = strArr(TianChengConstant.TRANS_TIME)
        val trans_type1 = strArr(TianChengConstant.TRANS_TYPE1)
        val trans_type2 = strArr(TianChengConstant.TRANS_TYPE2)
        (uid, trans_channel, trans_day, trans_amt, trans_time, trans_type1, trans_type2)
    }
    trans_base_cols
  }

  def get_all_users(sc: SparkContext, op_base_cols_input_path: String,
                    transaction_output_path: String): RDD[String] = {
    val op_users = sc.textFile(op_base_cols_input_path).map {
      line =>
        val uid = line.split("\\|")(0)
        uid
    }

    val trans_users = sc.textFile(transaction_output_path).map {
      line =>
        val uid = line.split("\\|")(0)
        uid
    }

    val all_users = op_users.union(trans_users).distinct()
    all_users
  }

  def get_op_type_id_map(sc: SparkContext, op_type2Index_input_path: String): Map[String, String] = {
    val op_type_id_map = sc.textFile(op_type2Index_input_path).map {
      line =>
        val strArr = line.split("\\|", -1)
        val op_type = strArr(0)
        val op_type_id = strArr(1)
        (op_type, op_type_id)
    }.collectAsMap()
    op_type_id_map
  }

  def get_trans_type_id_map(sc: SparkContext, trans_type2Index_input_path: String): Map[String, String] = {
    val trans_type_id_map = sc.textFile(trans_type2Index_input_path).map {
      line =>
        val strArr = line.split("\\|", -1)
        val trans_type = strArr(0)
        val trans_type_id = strArr(1)
        (trans_type, trans_type_id)
    }.collectAsMap()
    trans_type_id_map
  }
}
