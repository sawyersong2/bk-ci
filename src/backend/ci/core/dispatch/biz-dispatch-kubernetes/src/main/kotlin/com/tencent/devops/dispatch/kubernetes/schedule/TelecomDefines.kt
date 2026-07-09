package com.tencent.devops.scheduler.schedule

object TelecomDefines {
    // 运营商名称常量
    const val CHINA_TELECOM = "电信"
    const val CHINA_UNICOM = "联通"
    const val CHINA_MOBILE = "移动"
    const val DEFAULT_TELECOM = CHINA_TELECOM

    // 运营商完整名称
    const val TELECOM_OF_CHINA_TELECOM = "中国电信"
    const val TELECOM_OF_CHINA_UNICOM = "中国联通"
    const val TELECOM_OF_CHINA_MOBILE = "中国移动"
    const val TELECOM_OF_BGP = "BGP"

    // 位标志常量
    const val BIT_FLAG_OF_CHINA_TELECOM = 1 shl 1
    const val BIT_FLAG_OF_CHINA_UNICOM = 1 shl 2
    const val BIT_FLAG_OF_CHINA_MOBILE = 1 shl 3
    const val BIT_FLAG_OF_BGP = 1 shl 4

    /**
     * 解析运营商类型
     * @param telecom 运营商名称
     * @return Pair<运营商位标志, 是否匹配成功>
     */
    fun parseTelecomType(telecom: String): Pair<Int, Boolean> {
        return when (telecom) {
            TELECOM_OF_CHINA_TELECOM -> Pair(BIT_FLAG_OF_CHINA_TELECOM, true)
            TELECOM_OF_CHINA_UNICOM -> Pair(BIT_FLAG_OF_CHINA_UNICOM, true)
            TELECOM_OF_CHINA_MOBILE -> Pair(BIT_FLAG_OF_CHINA_MOBILE, true)
            else -> Pair(BIT_FLAG_OF_BGP, false)
        }
    }
}