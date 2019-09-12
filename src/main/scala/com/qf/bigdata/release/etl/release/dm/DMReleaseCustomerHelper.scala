package com.qf.bigdata.release.etl.release.dm

import scala.collection.mutable.ArrayBuffer

object DMReleaseCustomerHelper {

    def selectDMReleaseCustomerColumns():ArrayBuffer[String]={
        val columns = new ArrayBuffer[String]()
        columns.+=("release_session")
        columns.+=("release_status")
        columns.+=("device_num ")
        columns.+=("device_type")
        columns.+=("sources  ")
        columns.+=("channels  ")
        columns.+=(" idcard ")
        columns.+=("age")
        //加载udf函数
        columns.+=("getAgeRange(age) as age_range ")
        columns.+=("gender ")
        columns.+=("area_code ")
        columns.+=("ct")
        columns.+=("bdp_day")
        columns
    }
//渠道
    def selectDMReleaseCustomerColumns2():ArrayBuffer[String]= {
        val columns = new ArrayBuffer[String]()
        columns.+=("sources  ")
        columns.+=("channels  ")
        columns.+=(" device_type ")
        columns.+=(" user_count ")
        columns.+=(" total_count ")
        columns.+=(" bdp_day ")
        columns
    }

    /**
      * 目标多维
      */
    def selectDMCustomerCubeColumns():ArrayBuffer[String]= {
        val columns = new ArrayBuffer[String]()
        columns.+=("sources  ")
        columns.+=("channels  ")
        columns.+=(" device_type ")
        columns.+=(" age_range ")
        columns.+=(" gender ")
        columns.+=(" area_code ")
        columns.+=(" user_count ")
        columns.+=(" total_count ")
        columns.+=(" bdp_day ")
        columns
    }

}
