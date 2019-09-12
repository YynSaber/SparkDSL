package com.qf.bigdata.release.etl.release.dm

import scala.collection.mutable.ArrayBuffer

object DMExposureSourcesHelper {

    def selectDWReleaseExposure():ArrayBuffer[String]={
        val columns = new ArrayBuffer[String]()
        columns.+=("release_session")
        columns.+=("release_status")
        columns.+=("device_num ")
        columns.+=("device_type")
        columns.+=("sources  ")
        columns.+=("channels  ")
        columns.+=("ct")
        columns.+=("bdp_day")
        columns
    }

    def selectDMReleaseCustomerColumns():ArrayBuffer[String]= {
        val columns = new ArrayBuffer[String]()
        columns.+=("sources")
        columns.+=("channels  ")
        columns.+=(" device_type ")
        columns.+=(" user_count ")
        columns.+=(" bdp_day")
        columns
    }

    def selectDMExposureSource():ArrayBuffer[String]={
        val columns = new ArrayBuffer[String]()
        columns.+=("sources  ")
        columns.+=("channels  ")
        columns.+=(" device_type ")
        columns.+=(" exposure_count ")
        columns.+=("round(exposure_count/user_count,3) as exposure_rates ")
        columns.+=(" bdp_day ")
        columns
    }

    def selectMIDExposure():ArrayBuffer[String]={
        val columns = new ArrayBuffer[String]()
        columns.+=("device_num")
        columns.+=("device_type")
        columns.+=("sources")
        columns.+=("channels")
        columns.+=("getAgeRange(age) as age_range ")
        columns.+=("gender")
        columns.+=("area_code")
        columns.+=("bdp_day")
        columns
    }

    def selectDMCustomerCubeColumns():ArrayBuffer[String]= {
        val columns = new ArrayBuffer[String]()
        columns.+=("sources  ")
        columns.+=("channels  ")
        columns.+=(" device_type ")
        columns.+=(" age_range ")
        columns.+=(" gender ")
        columns.+=(" area_code ")
        columns.+=(" user_count ")
        columns.+=(" bdp_day ")
        columns
    }

    def selectDMExposureCubeColumns():ArrayBuffer[String]= {
        val columns = new ArrayBuffer[String]()
        columns.+=("sources")
        columns.+=("channels")
        columns.+=("device_type")
        columns.+=("age_range")
        columns.+=("gender")
        columns.+=("area_code")
        columns.+=("exposure_count")
        columns.+=("round(exposure_count/user_count,3) as exposure_rates")
        columns.+=(" bdp_day ")
        columns
    }


}
