package com.qf.bigdata.release.etl.release.ods

import scala.collection.mutable.ArrayBuffer

object ODSUserHelper {

    def selectODSUser():ArrayBuffer[String]={

        val columns = new ArrayBuffer[String]()
        columns.+=("user")
        columns.+=("action")
        columns.+=("action_count")
        columns.+=("case when action = '01' then 1 when action = '02' then 2 when action = '03' then 2 when action = '04' then 3 when action = '05' then 5 else 0 end as action_score")
        columns.+=("bdp_day")

    }
    def selectODSScore():ArrayBuffer[String]= {
        val columns = new ArrayBuffer[String]()
        columns.+=("user")
        columns.+=("case when uscore >= 1000 and uscore < 3000 then '银牌'when uscore >= 3000 and uscore < 10000 then '金牌' when uscore > 10000 then '钻石' else '普通' end as ulevel")
        columns.+=("bdp_day")
    }

}
