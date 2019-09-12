package com.qf.bigdata.release.constant

import org.apache.spark.sql.SaveMode
import org.apache.spark.storage.StorageLevel

/**
  * 常量工具类
  */
object ReleaseConstant {

  // partition
  val DEF_STORAGE_LEVEL :StorageLevel = StorageLevel.MEMORY_AND_DISK
  val DEF_SAVEMODE :SaveMode = SaveMode.Overwrite
  val DEF_PARTITION:String = "bdp_day"
  val DEF_SOURCE_PARTITIONS = 4

  // 维度列
  val COL_RELEASE_SESSION_STATUS :String = "release_status"
  val COL_RELEASE_SOURCES :String = "sources"
  val COL_RELEASE_CHANNELS :String = "channels"
  val COL_RELEASE_DEVICE_TYPE :String = "device_type"
  val COL_RELEASE_DEVICE_NUM :String = "device_num"
  val COL_RELEASE_IDCARD :String = "idcard"

    val COL_RELEASE_USER_COUNT :String = "user_count"
    val COL_RELEASE_TOTAL_COUNT :String = "total_count"
    val COL_RELEASE_EXPOSUER :String = "exposure_count"
    val COL_RELEASE_EXPOSUER_RATE :String = "exposure_rates"

    val COL_RELEASE_AGE_RANGE :String = "age_range"
    val COL_RELEASE_GENDER :String = "gender"
    val COL_RELEASE_AREA_CODE :String = "area_code"

  // ods==============================
  val ODS_RELEASE_SESSION = "ods_release.ods_01_release_session"

  // dw===============================
  val DW_RELEASE_CUSTOMER = "dw_release.dw_release_customer"

  val DW_RELEASE_EXPOSURE = "dw_release.dw_release_exposure"

    val  MID_RELEASE_EXPOSURE = "mid_release.mid_release_exposure"

    //dm===============================
    val DM_RELEASE_CUSTOMER_SOURCE = "dm_release.dm_customer_sources"

    val DM_RELEASE_CUSTOMER_CUBE = "dm_release.dm_customer_cube"

  val DM_EXPOSURE_SOURCES = "dm_release.dm_exposure_sources"

  val DM_EXPOSURE_CUBE = "dm_release.dm_exposure_cube"

}
