package tianchengDataMining

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory
import scopt.OptionParser
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.configuration.Algo
import org.apache.spark.mllib.tree.impurity.Entropy
import org.apache.spark.mllib.tree.model.DecisionTreeModel
import org.apache.spark.rdd.RDD

object Train_model {
  val logger = LoggerFactory.getLogger(getClass)
  Logger.getLogger("org").setLevel(Level.ERROR)

  def run(params: Params): Unit = {
    val spark = SparkSession
      .builder()
      .appName("train_model")
      .master("local[2]")
      .getOrCreate()

    val sc = spark.sparkContext
    remove_train_model_dirs(params)
    logger.info("Step_2: 模型训练")

    logger.info("获取融合特征数据...")
    val merged_features = sc.textFile(params.merged_features_input_path)
    BaseUtils.print_rdd(merged_features.map(_.toString), "merged_features")
    println("特征维度: " + (merged_features.first().split("\\|").length - 1))
    logger.info("分割数据集...")
    val split_data = merged_features.randomSplit(Array(0.8, 0.2))

    // 训练集数据
    val train_data = split_data(0).map {
      line =>
        val strArr = line.split("\\|", -1)
        val label = strArr(1).toDouble
        val features_cols = strArr.slice(2, strArr.length - 1).map {
          x => if (x.equals("")) 0.0 else x.toDouble
        }
        val features = Vectors.dense(features_cols)
        LabeledPoint(label, features)
    }

    // 测试集数据
    val test_data = split_data(1).map {
      line =>
        val strArr = line.split("\\|", -1)
        val uid = strArr(0)
        val label = strArr(1).toDouble
        val features_cols = strArr.slice(2, strArr.length - 1).map {
          x => if (x.equals("")) 0.0 else x.toDouble
        }
        val features = Vectors.dense(features_cols)
        (uid, label, features)
    }

    logger.info("训练决策树模型...")
    val dTree_model = train_dTree_model(train_data)
    logger.info(s"save DecisionTree model to : ${params.decision_tree_model_output_path}")
    // 保存决策树模型
    dTree_model.save(sc, params.decision_tree_model_output_path)

    // 决策树模型预测结果
    val test_result = test_data.map {
      case (uid, label, features) =>
        val prediction = dTree_model.predict(features)
        (uid, label, prediction)
    }


    // 决策树模型预测准确率
    val prediction_accuracy = test_result.filter(r => r._2 == r._3).count().toDouble / test_result.count()
    println(s"dTree_model's prediction accuracy: $prediction_accuracy")

  }

  // 训练决策树模型
  def train_dTree_model(train_data: RDD[LabeledPoint]): DecisionTreeModel = {
    val numClasses = 2
    val categoricalFeaturesInfo = Map[Int, Int]()
    val impurity = "gini"
    val maxDepth = 5
    val maxBins = 32

    val dTree_model = DecisionTree.trainClassifier(train_data, numClasses, categoricalFeaturesInfo,
      impurity, maxDepth, maxBins)
    dTree_model
  }


  def remove_train_model_dirs(params: Params): Unit = {
    BaseUtils.remove_existed_dir(params.decision_tree_model_output_path)
  }

  def main(args: Array[String]): Unit ={

    val defaultParams = Params()
    val parser = new OptionParser[Params]("preprocess"){
      opt[String]("merged_features_input_path")
        .action((x, c) => (c.copy(merged_features_input_path = x)))
      opt[String]("decision_tree_model_output_path")
        .action((x, c) => (c.copy(decision_tree_model_output_path = x)))

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
                     decision_tree_model_output_path: String = "/home/chroot/test_data/tiancheng_data/step_2/dTree_model"
                   )
}
