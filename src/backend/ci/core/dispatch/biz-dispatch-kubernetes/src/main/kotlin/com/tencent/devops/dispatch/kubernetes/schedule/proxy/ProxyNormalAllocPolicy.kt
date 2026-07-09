package com.tencent.devops.scheduler.schedule.proxy

import com.tencent.devops.scheduler.schedule.Policy
import com.tencent.devops.scheduler.schedule.ScheduleContext
import org.springframework.stereotype.Component

@Component
class ProxyNormalAllocPolicy : Policy{
    override fun execute(context: ScheduleContext): Any? {
        val proxyList = context.filterMgr?.run(context, emptyList())
        return context.allocMgr?.run(context, proxyList ?: emptyList())
    }

    override fun name(): String {
        return "ProxyNormalAllocPolicy"
    }

    override fun init() {
    }

    override fun isUse(context: ScheduleContext): Boolean {
        return true
    }
}