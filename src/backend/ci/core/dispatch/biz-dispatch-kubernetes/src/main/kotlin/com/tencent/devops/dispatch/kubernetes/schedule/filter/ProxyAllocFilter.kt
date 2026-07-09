package com.tencent.devops.scheduler.schedule.filter

import com.tencent.devops.scheduler.pojo.proxy.AllocProxyResponse
import com.tencent.devops.scheduler.schedule.PolicyManager
import com.tencent.devops.scheduler.schedule.ResourceFilter
import com.tencent.devops.scheduler.schedule.ScheduleContext
import com.tencent.devops.scheduler.schedule.profile.MutableSearchProfile
import com.tencent.devops.scheduler.schedule.proxy.ProxyAppPermissionFilter
import com.tencent.devops.scheduler.schedule.proxy.ProxyCdsDialScoreFilter
import com.tencent.devops.scheduler.schedule.proxy.ProxyIsoTagFilter
import com.tencent.devops.scheduler.schedule.proxy.ProxyNormalAllocPolicy
import com.tencent.devops.scheduler.schedule.proxy.ProxyNormalAllocator
import com.tencent.devops.scheduler.schedule.proxy.ProxyRedisZsetFilter
import com.tencent.devops.scheduler.schedule.proxy.ProxyTagFilter
import com.tencent.devops.scheduler.schedule.proxy.ProxyTelecomFilter
import com.tencent.devops.scheduler.schedule.proxy.ProxyTreeFilter
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Component

@Component
class ProxyAllocFilter @Autowired constructor(
    private val proxyNormalAllocPolicy: ProxyNormalAllocPolicy,
    private val proxyNormalAllocator: ProxyNormalAllocator,
    private val proxyTreeFilter: ProxyTreeFilter,
    private val proxyAppPermissionFilter: ProxyAppPermissionFilter,
    private val proxyRedisZsetFilter: ProxyRedisZsetFilter,
    private val proxyCdsDialScoreFilter: ProxyCdsDialScoreFilter,
    private val proxyTelecomFilter: ProxyTelecomFilter,
    private val proxyTagFilter: ProxyTagFilter,
    private val proxyIsoTagFilter: ProxyIsoTagFilter
): ResourceFilter  {

    override fun filter(context: ScheduleContext, inList: List<String>): List<String> {
        context.proxyRegion = context.req.zoneId
        if (context.searchProfile == null) {
            context.searchProfile = MutableSearchProfile()
        }

        val builder = PolicyManager.newBuild()
        val mgr = builder
            .add(
                proxyNormalAllocPolicy,
                proxyNormalAllocator,
                proxyTreeFilter,
                proxyAppPermissionFilter,
                proxyRedisZsetFilter,
                proxyCdsDialScoreFilter,
                proxyTelecomFilter,
                proxyTagFilter,
                proxyIsoTagFilter
            )
            .build()

        val result = mgr.innerRun(context.copy())
        context.allocProxyResponse = result as AllocProxyResponse
        context.skipProxyAllocFilter = true

        return inList
    }

    override fun name(): String {
        return PROXY_ALLOC_FILTER
    }

    override fun init() {

    }

    override fun isUse(context: ScheduleContext): Boolean {
        if (context.req.userId.isBlank()) {
            return false
        }

        // 只运行一次proxyAllocFilter
        if (context.skipProxyAllocFilter) {
            return false
        }

        return true
    }

    companion object {
        const val PROXY_ALLOC_FILTER = "proxy_alloc_filter"
        private val logger = LoggerFactory.getLogger(ProxyAllocFilter::class.java)
    }
}
