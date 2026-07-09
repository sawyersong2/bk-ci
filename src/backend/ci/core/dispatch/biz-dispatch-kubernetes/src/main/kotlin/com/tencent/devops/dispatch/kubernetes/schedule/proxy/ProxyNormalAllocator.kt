package com.tencent.devops.scheduler.schedule.proxy

import com.tencent.devops.common.api.exception.ErrorCodeException
import com.tencent.devops.common.redis.CommonRedisKey
import com.tencent.devops.common.redis.RedisOperation
import com.tencent.devops.scheduler.constant.ErrorMessageCode
import com.tencent.devops.scheduler.dao.ProxyDao
import com.tencent.devops.scheduler.pojo.proxy.AllocProxyResponse
import com.tencent.devops.scheduler.pojo.proxy.ProxyInfo
import com.tencent.devops.scheduler.pojo.proxy.ProxyLinkType
import com.tencent.devops.scheduler.pojo.proxy.TelecomType
import com.tencent.devops.scheduler.redis.ZSetRedisUtils
import com.tencent.devops.scheduler.schedule.Allocator
import com.tencent.devops.scheduler.schedule.ScheduleContext
import com.tencent.devops.scheduler.schedule.TelecomDefines.BIT_FLAG_OF_BGP
import com.tencent.devops.scheduler.schedule.TelecomDefines.BIT_FLAG_OF_CHINA_MOBILE
import com.tencent.devops.scheduler.schedule.TelecomDefines.BIT_FLAG_OF_CHINA_TELECOM
import com.tencent.devops.scheduler.schedule.TelecomDefines.BIT_FLAG_OF_CHINA_UNICOM
import com.tencent.devops.scheduler.schedule.strategy.StrategyAwareProxySelector
import org.jooq.DSLContext
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.stereotype.Component
import java.time.Instant

@Component
class ProxyNormalAllocator  @Autowired constructor(
    private val zSetRedisUtils: ZSetRedisUtils,
    private val redisOperation: RedisOperation,
    @Qualifier("cloudgameDslContext")
    private val cloudgameDslContext: DSLContext,
    private val proxyDao: ProxyDao,
    private val strategyAwareProxySelector: StrategyAwareProxySelector
): Allocator {
    companion object {
        private val logger = LoggerFactory.getLogger(ProxyNormalAllocator::class.java.name)
    }

    override fun alloc(context: ScheduleContext, inList: List<String>): Any {
        logger.info("ProxyNormalAllocator: ${context.req.userId}|${context.req.clientType}, inList: $inList, proxyRegion: ${context.proxyRegion}")
        context.searchProfile?.finalSelectionExecuted = true
        
        if (inList.isEmpty()) {
            logger.warn("ProxyNormalAllocator: ${context.req.userId}|${context.req.clientType}, no available proxy")
            throw ErrorCodeException(
                errorCode = ErrorMessageCode.SCHEDULE_ERROR,
                defaultMessage = "no available proxy"
            )
        }
        
        return allocFromProxyList(context, inList)
    }

    /**
     * 从指定的proxy列表中分配proxy。
     *
     * 选择算法由 [StrategyAwareProxySelector] 编排：
     * - kill switch 关闭 / 灰度未命中 / 单候选 → 行为与变更前一致（random）。
     * - 命中 weighted_v1 灰度 → 按 ZSet 综合分加权随机选择。
     * 埋点字段（`finalSelectionMethod` / `strategyVersion` / `strategyHit` / `reasonCodes`）由 selector 写入；
     * 本方法只补充与会话相关的 `selectedProxyIp` 与 ZSet 预占用记录。
     */
    private fun allocFromProxyList(context: ScheduleContext, proxyList: List<String>): AllocProxyResponse {
        context.searchProfile?.finalCandidates = proxyList.size
        val outcome = strategyAwareProxySelector.select(
            zoneId = context.proxyRegion,
            candidates = proxyList,
            profile = context.searchProfile
        )
        val proxyIp = outcome.proxyIp
        context.searchProfile?.selectedProxyIp = proxyIp

        val preUserKey = CommonRedisKey.redisKeyOfProxyPreUsers(proxyIp)
        // 历史原因，使用毫秒作为过期时间
        redisOperation.zadd(
            preUserKey,
            "${context.req.userId}|${context.req.clientType}",
            System.currentTimeMillis().toDouble()
        )

        logger.info("ProxyNormalAllocator: ${context.req.userId}|${context.req.clientType}, " +
                "selected proxy: $proxyIp via ${outcome.method} (from ${proxyList.size} proxies)")

        val proxyCfg = proxyDao.getProxyDataByInnerIp(cloudgameDslContext, proxyIp)
        if (proxyCfg == null) {
            logger.error("ProxyNormalAllocator error, proxy not found for $proxyIp")
            throw ErrorCodeException(
                errorCode = ErrorMessageCode.SCHEDULE_ERROR,
                defaultMessage = "proxy not found for $proxyIp"
            )
        }

        return buildResponse(context.telecomType, proxyCfg)
    }


    private fun buildResponse(
        telecom: Int,
        proxyCfg: ProxyTableData
    ): AllocProxyResponse {
        val baseInfo = createBaseProxyInfo(proxyCfg)

        return when (telecom) {
            BIT_FLAG_OF_CHINA_TELECOM ->
                handleChinaTelecom(proxyCfg, baseInfo)
            BIT_FLAG_OF_CHINA_UNICOM ->
                handleChinaUnicom(proxyCfg, baseInfo)
            BIT_FLAG_OF_CHINA_MOBILE ->
                handleChinaMobile(proxyCfg, baseInfo)
            BIT_FLAG_OF_BGP ->
                handleBgp(proxyCfg, baseInfo)
            else -> throw IllegalArgumentException("Unknown telecom type")
        }
    }

    private fun handleChinaTelecom(
        cfg: ProxyTableData,
        baseInfo: ProxyInfo
    ): AllocProxyResponse {
        val updatedInfo = baseInfo.copy(
            telecomType = TelecomType.CHINA_TELECOM
        ).takeIf { cfg.telecomEndpoint.isNotEmpty() }?.let {
            it.copy(
                endPoint = cfg.telecomEndpoint,
                linkType = ProxyLinkType.values().getOrElse(cfg.telecomLinkType) {
                    ProxyLinkType.PROXY_LINK_TYPE_DIRECT
                }
            )
        } ?: baseInfo

        return AllocProxyResponse(
            code = 0,
            message = "success",
            defaultProxy = updatedInfo,
            others = listOfNotNull(
                fillProxyInfo(cfg, TelecomType.CHINA_UNICOM),
                fillProxyInfo(cfg, TelecomType.CHINA_MOBILE),
                fillProxyInfo(cfg, TelecomType.BGP)
            )
        )
    }

    private fun handleChinaUnicom(
        cfg: ProxyTableData,
        baseInfo: ProxyInfo
    ): AllocProxyResponse {
        val updatedInfo = baseInfo.copy(
            telecomType = TelecomType.CHINA_UNICOM
        ).takeIf { cfg.unicomEndpoint.isNotEmpty() }?.let {
            it.copy(
                endPoint = cfg.unicomEndpoint,
                linkType = ProxyLinkType.values().getOrElse(cfg.unicomLinkType) {
                    ProxyLinkType.PROXY_LINK_TYPE_DIRECT
                }
            )
        } ?: baseInfo

        return AllocProxyResponse(
            code = 0,
            message = "success",
            defaultProxy = updatedInfo,
            others = listOfNotNull(
                fillProxyInfo(cfg, TelecomType.CHINA_TELECOM),
                fillProxyInfo(cfg, TelecomType.CHINA_MOBILE),
                fillProxyInfo(cfg, TelecomType.BGP)
            )
        )
    }

    private fun handleChinaMobile(
        cfg: ProxyTableData,
        baseInfo: ProxyInfo
    ): AllocProxyResponse {
        val updatedInfo = baseInfo.copy(
            telecomType = TelecomType.CHINA_MOBILE
        ).takeIf { cfg.mobileEndpoint.isNotEmpty() }?.let {
            it.copy(
                endPoint = cfg.mobileEndpoint,
                linkType = ProxyLinkType.values().getOrElse(cfg.mobileLinkType) {
                    ProxyLinkType.PROXY_LINK_TYPE_DIRECT
                }
            )
        } ?: baseInfo

        return AllocProxyResponse(
            code = 0,
            message = "success",
            defaultProxy = updatedInfo,
            others = listOfNotNull(
                fillProxyInfo(cfg, TelecomType.CHINA_TELECOM),
                fillProxyInfo(cfg, TelecomType.CHINA_UNICOM),
                fillProxyInfo(cfg, TelecomType.BGP)
            )
        )
    }

    private fun handleBgp(
        cfg: ProxyTableData,
        baseInfo: ProxyInfo
    ): AllocProxyResponse {
        val updatedInfo = baseInfo.copy(
            telecomType = TelecomType.BGP
        ).takeIf { cfg.bgpEndpoint.isNotEmpty() }?.let {
            it.copy(
                endPoint = cfg.bgpEndpoint,
                linkType = ProxyLinkType.values().getOrElse(cfg.bgpLinkType) {
                    ProxyLinkType.PROXY_LINK_TYPE_DIRECT
                }
            )
        } ?: baseInfo

        return AllocProxyResponse(
            code = 0,
            message = "success",
            defaultProxy = updatedInfo,
            others = listOfNotNull(
                fillProxyInfo(cfg, TelecomType.CHINA_TELECOM),
                fillProxyInfo(cfg, TelecomType.CHINA_UNICOM),
                fillProxyInfo(cfg, TelecomType.CHINA_MOBILE)
            )
        )
    }

    private fun createBaseProxyInfo(cfg: ProxyTableData) =
        ProxyInfo(
            proxyId = cfg.proxyId,
            innerIp = cfg.innerIp,
            telecomType = TelecomType.CHINA_TELECOM,
            endPoint = cfg.endpoint,
            cdsEndpoint = cfg.cdsEndpoint,
            linkType = ProxyLinkType.PROXY_LINK_TYPE_DIRECT,
            proxyVersion = cfg.version
        )

    private fun fillProxyInfo(
        cfg: ProxyTableData,
        type: TelecomType
    ): ProxyInfo? {
        return when (type) {
            TelecomType.CHINA_TELECOM ->
                cfg.telecomEndpoint.takeIf { it.isNotEmpty() }?.let {
                    ProxyInfo(
                        proxyId = cfg.proxyId,
                        innerIp = cfg.innerIp,
                        telecomType = type,
                        endPoint = it,
                        cdsEndpoint = cfg.cdsEndpoint,
                        linkType = ProxyLinkType.values().getOrElse(cfg.telecomLinkType) {
                            ProxyLinkType.PROXY_LINK_TYPE_DIRECT
                        },
                        proxyVersion = cfg.version
                    )
                }
            TelecomType.CHINA_UNICOM ->
                cfg.unicomEndpoint.takeIf { it.isNotEmpty() }?.let {
                    ProxyInfo(
                        proxyId = cfg.proxyId,
                        innerIp = cfg.innerIp,
                        telecomType = type,
                        endPoint = it,
                        cdsEndpoint = cfg.cdsEndpoint,
                        linkType = ProxyLinkType.values().getOrElse(cfg.unicomLinkType) {
                            ProxyLinkType.PROXY_LINK_TYPE_DIRECT
                        },
                        proxyVersion = cfg.version
                    )
                }
            TelecomType.CHINA_MOBILE ->
                cfg.mobileEndpoint.takeIf { it.isNotEmpty() }?.let {
                    ProxyInfo(
                        proxyId = cfg.proxyId,
                        innerIp = cfg.innerIp,
                        telecomType = type,
                        endPoint = it,
                        cdsEndpoint = cfg.cdsEndpoint,
                        linkType = ProxyLinkType.values().getOrElse(cfg.mobileLinkType) {
                            ProxyLinkType.PROXY_LINK_TYPE_DIRECT
                        },
                        proxyVersion = cfg.version
                    )
                }
            TelecomType.BGP ->
                cfg.bgpEndpoint.takeIf { it.isNotEmpty() }?.let {
                    ProxyInfo(
                        proxyId = cfg.proxyId,
                        innerIp = cfg.innerIp,
                        telecomType = type,
                        endPoint = it,
                        cdsEndpoint = cfg.cdsEndpoint,
                        linkType = ProxyLinkType.values().getOrElse(cfg.bgpLinkType) {
                            ProxyLinkType.PROXY_LINK_TYPE_DIRECT
                        },
                        proxyVersion = cfg.version
                    )
                }
            else -> null
        }
    }

    override fun name(): String {
        return "ProxyNormalAllocator"
    }

    override fun init() {
    }

    override fun isUse(context: ScheduleContext): Boolean {
        return true
    }

    fun getProxyOrderByUserCount(proxyList: List<String>): String {
        val redisKeys = proxyList.flatMap { listOf(
            CommonRedisKey.redisKeyOfProxyUsers(it),
            CommonRedisKey.redisKeyOfProxyPreUsers(it)
        )}
        logger.info("redis keys: $redisKeys")
        val proxyUserCountMap = zSetRedisUtils.zCardMulti(redisKeys)
        logger.info("proxy user count map: $proxyUserCountMap")
        val initialSelection = proxyList.random()
        return proxyList.minByOrNull { proxy ->
            val proxyTableData = proxyDao.getProxyDataByInnerIp(cloudgameDslContext, proxy)
                ?: throw ErrorCodeException(
                    errorCode = ErrorMessageCode.SCHEDULE_ERROR,
                    defaultMessage = "Proxy[$proxy] not exist."
                )
            calculateLoadRate(proxyUserCountMap, proxy, proxyTableData)
        } ?: initialSelection
    }

    private fun calculateLoadRate(
        userCounts: Map<String, Long>,
        proxy: String,
        cfg: ProxyTableData
    ): Long {
        logger.info("proxy[$proxy] user counts: $userCounts")
        val totalUsers = userCounts.getOrDefault(CommonRedisKey.redisKeyOfProxyUsers(proxy), 0L) +
                userCounts.getOrDefault(CommonRedisKey.redisKeyOfProxyPreUsers(proxy), 0L)

        val rate = (totalUsers * 1000) / cfg.bandwidthLimit
        logger.info("proxy[$proxy] total users: $totalUsers, bandwidth limit: ${cfg.bandwidthLimit}, rate: $rate")
        return rate
    }
}