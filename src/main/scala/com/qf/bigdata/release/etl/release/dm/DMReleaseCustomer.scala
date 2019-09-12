package com.qf.bigdata.release.etl.release.dm

import com.qf.bigdata.release.constant.ReleaseConstant
import com.qf.bigdata.release.util.SparkHelper
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Column, DataFrame, SaveMode, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable.ArrayBuffer

class DMReleaseCustomer {

}

object DMReleaseCustomer {

    /**
      * DM投放目标客户数据集市
      */
    // 日志处理
    val logger: Logger = LoggerFactory.getLogger(DMReleaseCustomer.getClass)

    /**
      * 目标客户
      * status = "01"
      */
    def handleReleaseJob(spark: SparkSession, appName: String, bdp_day: String): Unit = {

        try {
            import spark.implicits._
            import org.apache.spark.sql.functions._
            //缓存级别
            val saveMode: SaveMode = SaveMode.Overwrite
            val storageLevel: StorageLevel = ReleaseConstant.DEF_STORAGE_LEVEL
            //当天日志数据
            val cusomerColumns: ArrayBuffer[String] = DMReleaseCustomerHelper.selectDMReleaseCustomerColumns()
            //条件
            val cusomerCondition: Column = $"${ReleaseConstant.DEF_PARTITION}" === lit(bdp_day)
            val customerReleaseDF: DataFrame = SparkHelper.readTableData(spark, ReleaseConstant.DW_RELEASE_CUSTOMER)
                    .where(cusomerCondition)
                    .selectExpr(cusomerColumns: _*)
                    .persist(storageLevel)
//            println("查询结束======================结果显示")
//            customerReleaseDF.show(10, false)

            // 统计渠道指标
            val customerSourceGroupColumns: Seq[Column] = Seq[Column](
                $"${ReleaseConstant.COL_RELEASE_SOURCES}",
                $"${ReleaseConstant.COL_RELEASE_CHANNELS}"
                , $"${ReleaseConstant.COL_RELEASE_DEVICE_TYPE}")
            //插入表的select语句
            val customerSourceColnmns: ArrayBuffer[String] = DMReleaseCustomerHelper.selectDMReleaseCustomerColumns2()
            //按照条件
            val customerSourceDF: DataFrame = customerReleaseDF
                    .groupBy(customerSourceGroupColumns: _*)
                    //lit() => 获取字面量
                    .agg(
                countDistinct(col(ReleaseConstant.COL_RELEASE_DEVICE_NUM))
                        .alias(s"${ReleaseConstant.COL_RELEASE_USER_COUNT}"),
                count(col(ReleaseConstant.COL_RELEASE_DEVICE_NUM))
                        .alias(s"${ReleaseConstant.COL_RELEASE_TOTAL_COUNT}")
            )
                    .withColumn(s"${ReleaseConstant.DEF_PARTITION}", lit(bdp_day))
                    .selectExpr(customerSourceColnmns: _*)
                        println("查询结束======================结果显示")
                        customerSourceDF.show(10, false)
//                SparkHelper.writeTableData(customerSourceDF,ReleaseConstant.DM_RELEASE_CUSTOMER_SOURCE,saveMode)

            //目标客户多为统计
            val customerCubeGroupColumns: Seq[Column] = Seq[Column](
                $"${ReleaseConstant.COL_RELEASE_SOURCES}",
                $"${ReleaseConstant.COL_RELEASE_CHANNELS}",
                $"${ReleaseConstant.COL_RELEASE_DEVICE_TYPE}",
                $"${ReleaseConstant.COL_RELEASE_AGE_RANGE}",
                $"${ReleaseConstant.COL_RELEASE_GENDER}",
                $"${ReleaseConstant.COL_RELEASE_AREA_CODE}"
            )

            //插入表的select语句
            val customeCubeColnmns: ArrayBuffer[String] = DMReleaseCustomerHelper.selectDMCustomerCubeColumns()
            val customerCubeDF = customerReleaseDF
                    .cube(customerCubeGroupColumns: _*)
                    .agg(
                        countDistinct(col(ReleaseConstant.COL_RELEASE_DEVICE_NUM))
                                .alias(s"${ReleaseConstant.COL_RELEASE_USER_COUNT}"),
                        count(col(ReleaseConstant.COL_RELEASE_DEVICE_NUM))
                                .alias(s"${ReleaseConstant.COL_RELEASE_TOTAL_COUNT}")
                    )
                    .withColumn(s"${ReleaseConstant.DEF_PARTITION}", lit(bdp_day))
                    .selectExpr(customeCubeColnmns: _*)

            println("查询结束======================结果显示")
            customerCubeDF.show(10, false)
//                SparkHelper.writeTableData(customerCubeDF,ReleaseConstant.DM_RELEASE_CUSTOMER_CUBE,saveMode)

        } catch {
            case ex: Exception => {
                logger.error(ex.getMessage, ex)
            }
        }
    }

    /**
      * 投放目标客户
      */
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
        val appName: String = "dm_customer_job"
        val bdp_day_begin: String = "2019-09-06"
        val bdp_day_end: String = "2019-09-06"
        // 执行Job
        handleJobs(appName, bdp_day_begin, bdp_day_end)
    }
}