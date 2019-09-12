package com.qf.bigdata.release.etl.release.mid

import scala.collection.mutable.ArrayBuffer

object MIDReleaseExposureHelper {

    def selectMIDReleaseColumns1():ArrayBuffer[String]={
        val columns = new ArrayBuffer[String]()
        columns.+=("release_session")
        columns.+=("release_status")
        columns.+=("device_num")
        columns.+=("device_type")
        columns.+=("sources")
        columns.+=("channels")
//        columns.+=("idcard")
//        columns.+=("age")
//        columns.+=("gender")
//        columns.+=("area_code")
//        columns.+=("longitude")
//        columns.+=("latitude")
//        columns.+=("matter_id")
//        columns.+=("model_code")
//        columns.+=("model_version")
//        columns.+=("aid")
        columns.+=("ct")
        columns.+=("bdp_day")
        columns
    }

    def selectMIDReleaseColumns2():ArrayBuffer[String]={
        val columns = new ArrayBuffer[String]()
       columns.+=("release_session")
       columns.+=("get_json_object(exts,'$.idcard') as idcard")
       columns.+=("floor(datediff(from_unixtime(unix_timestamp(),'yyyy-MM-dd')," +
                "from_unixtime(unix_timestamp(substr(get_json_object(exts,'$.idcard'),7,8),'yyyyMMdd')))/365) as age")
        columns.+=("case subStr(get_json_object(exts,'$.idcard'),17,1)%2 " +
                "when 1 then '男'  when 0 then '女' end as gender ")
        columns.+=("get_json_object(exts,'$.area_code') as area_code")
        columns.+=("get_json_object(exts,'$.longitude') as longitude")
        columns.+=("get_json_object(exts,'$.latitude') as latitude")
        columns.+=("get_json_object(exts,'$.matter_id') as matter_id")
        columns.+=("get_json_object(exts,'$.model_code') as model_code")
        columns.+=("get_json_object(exts,'$.model_code') as model_version")
        columns.+=("get_json_object(exts,'$.aid') aid")
        columns
    }
}
