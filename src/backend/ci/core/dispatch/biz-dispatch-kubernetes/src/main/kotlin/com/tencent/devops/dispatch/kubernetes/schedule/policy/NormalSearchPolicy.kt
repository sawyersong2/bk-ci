package com.tencent.devops.scheduler.schedule.policy

import com.tencent.devops.common.api.exception.ErrorCodeException
import com.tencent.devops.scheduler.constant.ErrorMessageCode
import com.tencent.devops.scheduler.schedule.Policy
import com.tencent.devops.scheduler.schedule.ScheduleContext
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component

@Component
class NormalSearchPolicy : Policy {
    override fun execute(context: ScheduleContext): Any? {
        val cdsList = context.filterMgr?.run(context, emptyList())
        if(cdsList.isNullOrEmpty()) {
            logger.error("No cds found to schedule.")
            throw ErrorCodeException(
                errorCode = ErrorMessageCode.SCHEDULE_ERROR,
                defaultMessage = "No cds found to schedule."
            )
        }

        logger.info("cdsList: $cdsList")

        return context.allocMgr?.run(context, cdsList)
    }

    override fun name(): String {
        return "NormalSearchPolicy"
    }

    override fun init() {}

    override fun isUse(context: ScheduleContext): Boolean {
        return true
    }

    companion object {
        private val logger = LoggerFactory.getLogger(NormalSearchPolicy::class.java)
    }
}