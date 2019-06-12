package tianchengDataMining

import BaseUtils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory
import scopt.OptionParser
import org.apache.spark.mllib.feature.{Word2Vec, Word2VecModel}
import breeze.plot._
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ListBuffer

object SimpleEDA {
  val logger = LoggerFactory.getLogger(getClass)
  Logger.getLogger("org").setLevel(Level.ERROR)

  def run(params: Params): Unit = {
    val spark = SparkSession
      .builder()
      .appName("simple_eda")
      .master("local[2]")
      .getOrCreate()

    val sc = spark.sparkContext
    remove_simple_eda_dirs(params)

    val op_base_cols = BaseRDD.get_basic_operation_cols(sc, params.op_base_cols_input_path)
    BaseUtils.print_rdd(op_base_cols.map(_.toString), "op_base_cols")
    val trans_base_cols = BaseRDD.get_basic_trans_cols(sc, params.trans_base_cols_input_path)
    BaseUtils.print_rdd(trans_base_cols.map(_.toString), "trans_base_cols")

    val op_type_id_map = BaseRDD.get_op_type_id_map(sc, params.op_type2Index_input_path)
    val trans_type_id_map = BaseRDD.get_trans_type_id_map(sc, params.trans_type2Index_input_path)

    val op_single_day = op_base_cols.map {
      case (uid, op_day, op_type, op_time) =>
        ((uid, op_day.toInt), (op_type, op_time))
    }.groupByKey().mapValues {
      x =>
        val sorted_value = x.toVector
        val count = x.size
        sorted_value.map(x => (op_type_id_map(x._1), x._2))
    }
    BaseUtils.print_rdd(op_single_day.map(_.toString), "op_single_day")

    val trans_single_day = trans_base_cols.map {
      case (uid, trans_channel, trans_day, trans_amt, trans_time, trans_type1, trans_type2) =>
        ((uid, trans_day.toInt), (trans_type1, trans_time))
    }.groupByKey().mapValues {
      x =>
        val sorted_value = x.toVector
        val count = x.size
        sorted_value.map(x => (trans_type_id_map(x._1), x._2))
    }
    BaseUtils.print_rdd(trans_single_day.map(_.toString), "trans_single_day")

    val user_action_single_day = op_single_day.fullOuterJoin(trans_single_day).map {
      case ((uid, day), (op_types_time, None)) => (uid, day, op_types_time.get)
      case ((uid, day), (None, trans_type_time)) => (uid, day, trans_type_time.get)
      case ((uid, day), (op_types_time, trans_type_time)) =>
        (uid, day, Vector.concat(op_types_time.get, trans_type_time.get))
    }.map {
      line =>
        val action_time = line._3.sortBy(_._2).unzip
        //val min_time = action_time._2.min
        //val max_time = action_time._2.max
        (line._1, line._2, action_time._1.mkString("-"), action_time._2.mkString("-"))
    }.sortBy(x => (x._1, x._2))
    BaseUtils.print_rdd(user_action_single_day.map(_.toString), "user_action_single_day")
    user_action_single_day.map(BaseUtils.tupleToString(_, "|"))
      .saveAsTextFile(params.user_action_output_path)

    val user_tags = sc.textFile(params.tag_log_input_path).map {
      line =>
        val strArr = line.split("\\|", -1)
        val uid = strArr(0)
        val tag = strArr(1)
        (uid, tag)
    }

    val risky_users = user_tags.filter(_._2.toInt == 1)

    val no_risky_users = user_tags.filter(_._2.toInt == 0)

    val risky_action_single_day = user_action_single_day.map {
      case (uid, day, action_types, action_times) => (uid, (day, action_types, action_times))
    }.join(risky_users).map {
      case (uid, ((day, action_types, action_times), tag)) =>
        (uid, day, action_types, action_times)
    }
    BaseUtils.print_rdd(risky_action_single_day.map(_.toString), "risky_action_single_day")
    risky_action_single_day.map(BaseUtils.tupleToString(_, "|"))
      .saveAsTextFile(params.risky_user_action_output_path)

    val no_risky_action_single_day = user_action_single_day.map {
      case (uid, day, action_types, action_times) => (uid, (day, action_types, action_times))
    }.join(no_risky_users).map {
      case (uid, ((day, action_types, action_times), tag)) =>
        (uid, day, action_types, action_times)
    }
    BaseUtils.print_rdd(no_risky_action_single_day.map(_.toString), "no_risky_action_single_day")
    no_risky_action_single_day.map(BaseUtils.tupleToString(_, "|"))
      .saveAsTextFile(params.no_risky_user_action_output_path)

    //val user_action_data = get_plot_figure_data(user_action_single_day)

    //val risky_action_data = get_plot_figure_data(risky_action_single_day)

    //val no_risky_action_data = get_plot_figure_data(no_risky_action_single_day)
    /*
    data._1.foreach(x => print(x + " "))
    println()
    data._2.foreach(x => print(x + " "))
*/
    //show_action_type_figure(user_action_data, "/home/chroot/test_data/multi_source/img/lines.png")
    //show_action_type_figure(risky_action_data, "/home/chroot/test_data/multi_source/img/risky_action.png")
    //show_action_type_figure(no_risky_action_data, "/home/chroot/test_data/multi_source/img/no_risky_action.png")

    val user_action_rank = user_action_single_day.flatMap {
      case (uid, day, action_types, action_times) =>
        val cooccurrence_actions = get_cooccurrence_actions(action_types)
        cooccurrence_actions.map (x => (uid, x))
    }.map {
      case (uid, action) => ((uid, action), 1)
    }.reduceByKey(_ + _).map {
      case ((uid, action), count) => (uid, action, count)
    }.sortBy(x => (x._1, -x._3))
    BaseUtils.print_rdd(user_action_rank.map(_.toString), "user_action_rank")

    val risky_user_action_rank = user_action_rank.map {
      case (uid, action, count) => (uid, (action, count))
    }.join(risky_users).map {
      case (uid, ((action, count), tag)) => (uid, action, count)
    }
    BaseUtils.print_rdd(risky_user_action_rank.map(_.toString), "risky_user_action_rank")

    val no_risky_user_action_rank = user_action_rank.map {
      case (uid, action, count) => (uid, (action, count))
    }.join(no_risky_users).map {
      case (uid, ((action, count), tag)) => (uid, action, count)
    }
    BaseUtils.print_rdd(no_risky_user_action_rank.map(_.toString), "no_risky_user_action_rank")

    val related_action = get_related_action(user_action_rank)
    BaseUtils.print_rdd(related_action.map(_.toString), "related_action")
    related_action.coalesce(1).map(BaseUtils.tupleToString(_, "|"))
      .saveAsTextFile(params.related_action_output_path)

    val risky_related_action = get_related_action(risky_user_action_rank)
    BaseUtils.print_rdd(risky_related_action.map(_.toString), "risky_related_action")
    risky_related_action.coalesce(1).map(BaseUtils.tupleToString(_, "|"))
      .saveAsTextFile(params.risky_related_action_output_path)

    val no_risky_related_action = get_related_action(no_risky_user_action_rank)
    BaseUtils.print_rdd(no_risky_related_action.map(_.toString), "no_risky_ralted_action")
    no_risky_related_action.coalesce(1).map(BaseUtils.tupleToString(_, "|"))
      .saveAsTextFile(params.no_risky_related_action_output_path)

    val related_action_feature = user_action_rank.filter(_._2.split("-").length > 1).map {
      case (uid, actions, count) =>
        val actions_arr = actions.split("-")
        val action_1 = actions_arr(0)
        val action_2 = actions_arr(1)
        (uid, (action_1, action_2, count))
    }.groupByKey().mapValues {
      x =>
        val risky_action_1 = x.filter(_._1.equals("017"))
        val risky_value_1 = if (risky_action_1.nonEmpty) {
          val action_count_max = risky_action_1.maxBy(_._3)
          if (action_count_max._2.equals("001")||action_count_max._2.equals("040")) 1.00 else 0.00
        } else 0.00

        val risky_action_2 = x.filter(_._1.equals("102"))
        val risky_value_2 = if (risky_action_2.nonEmpty) {
          val action_count_max = risky_action_2.maxBy(_._3)
          if (action_count_max._2.equals("103")) 1.00 else 0.00
        } else 0.00

        val risky_value_3 = if (x.exists(_._1.toInt > 105) || x.exists(_._2.toInt > 105)) 0.00 else 1.00
        risky_value_1 + "|" + risky_value_2 + "|" + risky_value_3
    }.sortByKey()

    BaseUtils.print_rdd(related_action_feature.map(_.toString), "related_action_feature")
    related_action_feature.map(BaseUtils.tupleToString(_, "|"))
      .saveAsTextFile(params.related_action_feature_output_path)


  }


  def get_related_action(action_rank: RDD[(String, String, Int)]): RDD[(String, String)] = {
    val related_action = action_rank.filter(_._2.split("-").length > 1).map {
      case (uid, actions, count) =>
        val actions_arr = actions.split("-")
        val action_1 = actions_arr(0)
        val action_2 = actions_arr(1)
        ((action_1, action_2), 1)
    }.reduceByKey(_ + _).map {
      case ((action_1, action_2), count) => (action_1, (action_2, count))
    }.groupByKey().mapValues {
      x =>
        val action_count = x.toVector.unzip._2.sum
        val action_related = x.toVector.sortBy(-_._2).take(3).map{
          line =>
            val action_related_rate = (line._2.toDouble / action_count).formatted("%.2f")
            (line._1, action_related_rate)
        }
        action_related.mkString("[", ",", "]")
    }.sortByKey()
    related_action
  }

  def get_cooccurrence_actions(str: String): ListBuffer[String] ={
    val strArr = str.split("-")
    val cooccurrence_actions = new ListBuffer[String]()
    for (i <- 0 until strArr.length - 1) {
      if (!strArr(i).equals(strArr(i + 1))) {
        cooccurrence_actions.append(strArr(i) + "-" + strArr(i + 1))
      } else if (i + 1 >= strArr.length - 1) {
        cooccurrence_actions.append(strArr(i))
      }
    }
    if (strArr.length == 1) {
      cooccurrence_actions.append(strArr(0))
    }
    cooccurrence_actions
  }

  def get_plot_figure_data(action_data: RDD[(String, Int, String, String)]): Array[(Array[Double], Array[Double])] = {
    val data = action_data.filter(_._3.split("-").length > 20).map {
      line =>
        val action = line._3.split("-").map(_.toDouble)
        val time = line._4.split("-").map {
          x =>
            val hour = x.substring(0, 2).toDouble
            val minute = x.substring(3, 5).toDouble
            val second = x.substring(6, 8).toDouble
            val time = hour * 3600 + minute * 60 + second
            time
        }
        (time, action)
    }.collect.take(2)
    data
  }

  def show_action_type_figure(action_data: Array[(Array[Double], Array[Double])], img_path: String): Unit = {
    val figure = Figure()
    val p = figure.subplot(0)
    action_data.foreach {
      line =>
        val time_data = line._1
        val action_data = line._2
        p += plot(time_data, action_data, '.')
    }

    p.xlabel = "x axis"
    p.ylabel = "y axis"
    figure.saveas(img_path)
    println("saved")
  }

  def remove_simple_eda_dirs(params: Params): Unit = {
    BaseUtils.remove_existed_dir(params.user_action_output_path)
    BaseUtils.remove_existed_dir(params.risky_user_action_output_path)
    BaseUtils.remove_existed_dir(params.no_risky_user_action_output_path)
    BaseUtils.remove_existed_dir(params.related_action_output_path)
    BaseUtils.remove_existed_dir(params.risky_related_action_output_path)
    BaseUtils.remove_existed_dir(params.no_risky_related_action_output_path)
    BaseUtils.remove_existed_dir(params.related_action_feature_output_path)
  }

  def main(args: Array[String]): Unit ={

    val defaultParams = Params()
    val parser = new OptionParser[Params]("preprocess"){
      opt[String]("tag_log_input_path")
        .action((x, c) => (c.copy(tag_log_input_path = x)))
      opt[String]("op_base_cols_input_path")
        .action((x, c) => (c.copy(op_base_cols_input_path = x)))
      opt[String]("trans_base_cols_input_path")
        .action((x, c) => (c.copy(trans_base_cols_input_path = x)))
      opt[String]("op_type2Index_input_path")
        .action((x, c) => (c.copy(op_type2Index_input_path = x)))
      opt[String]("trans_type2Index_input_path")
        .action((x, c) => (c.copy(trans_type2Index_input_path = x)))
      opt[String]("user_action_output_path")
        .action((x, c) => (c.copy(user_action_output_path = x)))
      opt[String]("risky_user_action_output_path")
        .action((x, c) => (c.copy(risky_user_action_output_path = x)))
      opt[String]("no_risky_user_action_output_path")
        .action((x, c) => (c.copy(no_risky_user_action_output_path = x)))
      opt[String]("related_action_output_path")
        .action((x, c) => (c.copy(related_action_output_path = x)))
      opt[String]("risky_related_action_output_path")
        .action((x, c) => (c.copy(risky_related_action_output_path = x)))
      opt[String]("no_risky_related_action_output_path")
        .action((x, c) => (c.copy(no_risky_related_action_output_path = x)))
      opt[String]("related_action_feature_output_path")
        .action((x, c) => (c.copy(related_action_feature_output_path = x)))


      checkConfig(params => success)
    }

    parser.parse(args, defaultParams).map{
      params => run(params)
    }.getOrElse{
      System.exit(1)
    }

  }

  case class Params(
                     tag_log_input_path: String = "/home/chroot/test_data/tiancheng_data/step_0/tag_train",
                     op_base_cols_input_path: String = "/home/chroot/test_data/tiancheng_data/step_0/op_base_cols",
                     trans_base_cols_input_path: String = "/home/chroot/test_data/tiancheng_data/step_0/trans_base_cols",
                     op_type2Index_input_path: String = "/home/chroot/test_data/tiancheng_data/step_1/op_type2Index",
                     trans_type2Index_input_path: String = "/home/chroot/test_data/tiancheng_data/step_1/trans_type2Index",
                     user_action_output_path: String = "/home/chroot/test_data/tiancheng_data/step_2/user_actions",
                     risky_user_action_output_path: String = "/home/chroot/test_data/tiancheng_data/step_2/risky_user_actions",
                     no_risky_user_action_output_path: String = "/home/chroot/test_data/tiancheng_data/step_2/no_risky_user_actions",
                     related_action_output_path: String = "/home/chroot/test_data/tiancheng_data/step_2/related_action",
                     risky_related_action_output_path: String = "/home/chroot/test_data/tiancheng_data/step_2/risky_related_action",
                     no_risky_related_action_output_path: String = "/home/chroot/test_data/tiancheng_data/step_2/no_risky_related_action",
                     related_action_feature_output_path: String = "/home/chroot/test_data/tiancheng_data/step_2/related_action_feature"

                   )

}
