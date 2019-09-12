package com.qf.bigdata.release.etl.release.ods

import com.qf.bigdata.release.constant.ReleaseConstant
import com.qf.bigdata.release.etl.release.dm.DMExposureSources
import com.qf.bigdata.release.etl.release.dm.DMExposureSources.{handleReleaseJob, logger}
import com.qf.bigdata.release.etl.release.dw.DWReleaseCustomer.logger
import com.qf.bigdata.release.util.SparkHelper
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Column, DataFrame, SaveMode, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable.ArrayBuffer

class ODSUser {
}

object ODSUser {

    val logger: Logger = LoggerFactory.getLogger(DMExposureSources.getClass)

    def handleReleaseJob(spark: SparkSession, appNmae: String, bdp_day: String): Unit = {

        try {
            import spark.implicits._
            import org.apache.spark.sql.functions._
            //缓存级别
            val saveMode: SaveMode = SaveMode.Overwrite
            val storageLevel: StorageLevel = ReleaseConstant.DEF_STORAGE_LEVEL

            val odsUserColumns = ODSUserHelper.selectODSUser()

            //过滤条件
            val timeCondition = (col(s"$bdp_day")) === lit(bdp_day)
            //聚合分组列
            val actionClsGroupColumns: Seq[Column] = Seq[Column]($"users", $"action", $"bdp_day")

            val odsUserColumnsDF: DataFrame = SparkHelper.readTableData(spark, "ods_test.ods_user_action_log")
                    .where(timeCondition)
                    .groupBy(actionClsGroupColumns: _*)
                    .agg(count(lit("user")).alias("action_count"))
                    .selectExpr(odsUserColumns: _*)

            val odsUerScoreColumns: ArrayBuffer[String] = ODSUserHelper.selectODSScore()
            val userScoreDF = odsUserColumnsDF.groupBy(actionClsGroupColumns: _*)
                    .agg(sum(lit("action_score")).alias("score"))
                    .selectExpr(odsUerScoreColumns: _*)

            SparkHelper.writeTableData(userScoreDF,"dm_test.user_level",saveMode)

        } catch {
            // 错误信息处理
            case ex: Exception => {
                logger.error(ex.getMessage, ex)
            }
        }
    }

    def handleJobs(appName: String, bdp_day_begin: String, bdp_day_end: String): Unit = {
        var spark: SparkSession = null
        try {
            // spark 配置参数
            val conf = new SparkConf()
                    .set("hive.exec.dynamic.partition", "true")
                    .set("hive.exec.dynamic.partition.mode", "nonstrict")
                    .set("spark.sql.shuffle.partitions", "32")
                    .set("hive.merge.mapfiles", "true")
                    .set("hive.input.format", "org.apache.hadoop.hive.ql.io.CombineHiveInputFormat")
                    .set("spark.sql.autoBroadcastJoinThreshold", "50485760")
                    .set("spark.sql.crossJoin.enabled", "true")
                    //.set("spark.sql.warehouse.dir","hdfs://hdfsCluster/sparksql/db")
                    .setAppName(appName)
                    .setMaster("local[4]")
            // spark  上下文
            spark = SparkHelper.createSpark(conf)
            // 参数校验
            val timeRanges = SparkHelper.rangeDates(bdp_day_begin, bdp_day_end)
            for (bdp_day <- timeRanges.reverse) {
                val bdp_date = bdp_day.toString
                handleReleaseJob(spark, appName, bdp_date)
            }
        } catch {
            case ex: Exception => {
                logger.error(ex.getMessage, ex)
            }
        } finally {
            if (spark != null) {
                spark.stop()
            }
        }
    }

    def main(args: Array[String]): Unit = {
        // 如果没有Windows下的hadoop环境变量的话，需要内部执行，自己加载，如果有了，那就算了
        //    System.setProperty("hadoop.home.dir", "D:\\Huohu\\下载\\hadoop-common-2.2.0-bin-master")
        val appName :String = "ods_user_job"
        val bdp_day_begin:String ="2019-09-12"
        val bdp_day_end:String ="2019-09-12"
        // 执行Job
        handleJobs(appName,bdp_day_begin,bdp_day_end)
    }

}
