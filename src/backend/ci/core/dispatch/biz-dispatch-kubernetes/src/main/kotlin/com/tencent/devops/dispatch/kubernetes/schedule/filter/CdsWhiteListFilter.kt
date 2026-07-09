package com.tencent.devops.scheduler.schedule.filter

import com.tencent.devops.scheduler.schedule.ResourceFilter
import com.tencent.devops.scheduler.schedule.ScheduleContext
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component

@Component
class CdsWhiteListFilter : ResourceFilter  {

    override fun filter(context: ScheduleContext, inList: List<String>): List<String> {
        logger.info("CdsWhiteListFilter filter inList: ${inList.joinToString(",")}")
        return context.req.whiteListParam?.resourceList ?: return inList
    }

    override fun name(): String {
        return "CdsWhiteListFilter"
    }

    override fun init() {

    }

    override fun isUse(context: ScheduleContext): Boolean {
        return context.req.whiteListParam?.resourceList?.isEmpty() != true
    }

    companion object {
        private val logger = LoggerFactory.getLogger(CdsWhiteListFilter::class.java)
    }
}