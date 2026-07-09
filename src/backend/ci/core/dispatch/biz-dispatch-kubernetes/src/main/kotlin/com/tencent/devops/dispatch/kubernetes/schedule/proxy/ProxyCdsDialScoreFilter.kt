package com.tencent.devops.scheduler.schedule.proxy

import com.tencent.devops.common.redis.CommonRedisKey
import com.tencent.devops.common.redis.RedisOperation
import com.tencent.devops.scheduler.schedule.ResourceFilter
import com.tencent.devops.scheduler.schedule.ScheduleContext
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.data.redis.core.RedisTemplate
import org.springframework.stereotype.Component

/**
 * CDS拨测分数过滤器
 * 功能：
 * 1. 过滤掉拨测分数低于阈值的Proxy（默认50分）
 * 2. 按拨测分数降序排序，优先选择分数更高的Proxy
 * 
 * 拨测分数范围：0-100，分数越高表示网络质量越好
 * 
 * 使用场景：
 * - 当请求中包含cdsId时，根据该CDS的拨测结果过滤和排序Proxy
 * - 确保分配给用户的Proxy具有良好的网络连通性
 */
@Component
class ProxyCdsDialScoreFilter @Autowired constructor(
    private val redisOperation: RedisOperation
) : ResourceFilter {

    override fun filter(context: ScheduleContext, inList: List<String>): List<String> {
        context.searchProfile?.dialScoreExecuted = true
        if (inList.isEmpty()) {
            logger.warn("[ProxyCdsDialScoreFilter] input proxy list is empty, cdsId=${context.req.cdsId}")
            return inList
        }

        if (context.req.cdsId.isBlank()) {
            logger.warn("[ProxyCdsDialScoreFilter] cdsId is blank, skip filter, proxyCount=${inList.size}")
            context.searchProfile?.apply {
                dialScoreSkipped = true
                reasonCodes += "dial_score_skip:BLANK_CDS_ID"
            }
            return inList
        }

        try {
            // 获取CDS拨测分数ZSet的key
            val dialScoreKey = CommonRedisKey.redisKeyOfCdsDialScore(context.req.cdsId)

            // 检查dialScoreKey是否存在内容
            val keySize = redisOperation.zCard(dialScoreKey)
            if (keySize == null || keySize == 0L) {
                logger.info("[ProxyCdsDialScoreFilter] dialScoreKey has no content, skip filter, " +
                        "cdsId=${context.req.cdsId}, proxyCount=${inList.size}")
                context.searchProfile?.apply {
                    dialScoreSkipped = true
                    reasonCodes += "dial_score_skip:NO_SCORE_DATA"
                }
                return inList
            }

            // 获取所有Proxy的拨测分数
            val proxyScores = inList.mapNotNull { proxyId ->
                val score = redisOperation.zscore(dialScoreKey, proxyId)
                if (score != null && score > MIN_DIAL_SCORE) {
                    proxyId to score
                } else {
                    logger.debug("[ProxyCdsDialScoreFilter] filter out proxy, cdsId=${context.req.cdsId}, " +
                            "proxyId=$proxyId, score=$score")
                    null
                }
            }

            if (proxyScores.isEmpty()) {
                logger.warn("[ProxyCdsDialScoreFilter] all proxies filtered out, cdsId=${context.req.cdsId}, " +
                        "originalCount=${inList.size}, return original list")
                return inList
            }

            // 按分数降序排序，分数越高越优先
            val sortedProxies = proxyScores
                .sortedByDescending { it.second }
                .map { it.first }

            logger.info("[ProxyCdsDialScoreFilter] filter completed, cdsId=${context.req.cdsId}, originalCount=${inList.size}, " +
                    "filteredCount=${sortedProxies.size}, filteredOutCount=${inList.size - sortedProxies.size}, " +
                    "topScore=${proxyScores.maxOfOrNull { it.second }}, " +
                    "minScore=${proxyScores.minOfOrNull { it.second }}")
            context.searchProfile?.apply {
                dialScoreOut = sortedProxies.size
                reasonCodes += "dial_score:APPLIED"
            }
            
            return sortedProxies
        } catch (e: Exception) {
            logger.error("[ProxyCdsDialScoreFilter] filter error, cdsId=${context.req.cdsId}, error=${e.message}," +
                    " return original list", e)
            context.searchProfile?.apply {
                dialScoreSkipped = true
                reasonCodes += "dial_score_skip:ERROR"
            }
            return inList
        }
    }

    override fun name(): String {
        return "proxy_cds_dial_score_filter"
    }

    override fun init() {
        logger.info("[ProxyCdsDialScoreFilter] initialized, minDialScore=$MIN_DIAL_SCORE")
    }

    override fun isUse(context: ScheduleContext): Boolean {
        // 只有当cdsId不为空时才启用此过滤器
        val shouldUse = context.req.cdsId.isNotBlank()
        if (!shouldUse) {
            logger.debug("[ProxyCdsDialScoreFilter] skip filter, cdsId is blank")
            context.searchProfile?.apply {
                dialScoreSkipped = true
                reasonCodes += "dial_score_skip:BLANK_CDS_ID"
            }
        }

        // 默认不启用
        context.searchProfile?.apply {
            dialScoreSkipped = true
            reasonCodes += "dial_score_skip:DISABLED"
        }
        return false
    }

    companion object {
        private val logger = LoggerFactory.getLogger(ProxyCdsDialScoreFilter::class.java)

        // 最小拨测分数阈值，低于此分数的Proxy将被过滤
        private const val MIN_DIAL_SCORE = 50.0
    }
}
