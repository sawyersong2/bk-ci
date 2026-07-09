package com.tencent.devops.scheduler.schedule.filter

import com.tencent.devops.scheduler.schedule.ResourceFilter
import com.tencent.devops.scheduler.schedule.ScheduleContext
import org.springframework.stereotype.Component

/**
 * CGS上存在的单实例游戏过滤，云桌面下暂无此逻辑
 */
@Component
class SingleInstanceFilter : ResourceFilter  {

    override fun filter(context: ScheduleContext, inList: List<String>): List<String> {
        return inList
    }

    override fun name(): String {
        return "SingleInstanceFilter"
    }

    override fun init() {
    }

    override fun isUse(context: ScheduleContext): Boolean {
        return false
    }
}