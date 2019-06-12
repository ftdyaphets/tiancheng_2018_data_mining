package tianchengDataMining

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory
import scopt.OptionParser
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.model.DecisionTreeModel
import org.apache.spark.rdd.RDD

object Predict {
  val logger = LoggerFactory.getLogger(getClass)
  Logger.getLogger("org").setLevel(Level.ERROR)

  def run(params: Params): Unit = {
    val spark = SparkSession
      .builder()
      .appName("predict")
      .master("local[2]")
      .getOrCreate()

    val sc = spark.sparkContext
    remove_predict_dirs(params)
    logger.info("Step_3: 预测结果")

    logger.info("获取融合特征数据...")
    val merged_features = sc.textFile(params.merged_features_input_path)
    BaseUtils.print_rdd(merged_features.map(_.toString), "merged_features")

    // 测试集数据
    val test_data = merged_features.map {
      line =>
        val strArr = line.split("\\|", -1)
        val uid = strArr(0)
        val features_cols = strArr.slice(2, strArr.length - 1).map {
          x => if (x.equals("")) 0.0 else x.toDouble
        }
        val features = Vectors.dense(features_cols)
        (uid, features)
    }
    println(test_data.first())
    logger.info("加载决策树模型...")
    val dTree_model = DecisionTreeModel.load(sc, params.decision_tree_model_input_path)

    // 决策树模型预测结果
    val test_result = test_data.map {
      case (uid, features) =>
        val prediction = dTree_model.predict(features).toInt
        (uid, prediction)
    }
    println(test_result.first())
    logger.info(s"预测结果保存在 ${params.predict_result_output_path}")
    test_result.coalesce(1).map(BaseUtils.tupleToString(_, ","))
      .saveAsTextFile(params.predict_result_output_path)


  }

  def remove_predict_dirs(params: Params): Unit = {
    BaseUtils.remove_existed_dir(params.predict_result_output_path)
  }

  def main(args: Array[String]): Unit ={

    val defaultParams = Params()
    val parser = new OptionParser[Params]("preprocess"){

      opt[String]("merged_features_input_path")
        .action((x, c) => (c.copy(merged_features_input_path = x)))
      opt[String]("decision_tree_model_input_path")
        .action((x, c) => (c.copy(decision_tree_model_input_path = x)))
      opt[String]("predict_result_output_path")
        .action((x, c) => (c.copy(predict_result_output_path = x)))

      checkConfig(params => success)
    }

    parser.parse(args, defaultParams).map{
      params => run(params)
    }.getOrElse{
      System.exit(1)
    }

  }

  case class Params(
                     merged_features_input_path: String = "/home/chroot/test_data/tiancheng_data/step_2/merged_features",
                     decision_tree_model_input_path: String = "/home/chroot/test_data/tiancheng_data/step_2/dTree_model",
                     predict_result_output_path: String = "/home/chroot/test_data/tiancheng_data/step_3/dTree_model_result"
                   )
}
