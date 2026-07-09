package com.tencent.devops.scheduler.schedule.proxy

import com.tencent.devops.common.api.util.JsonUtil
import com.tencent.devops.scheduler.dao.ProxyDao
import com.tencent.devops.scheduler.pojo.ProxyStatus
import com.tencent.devops.scheduler.pojo.tree.DeviceTreeType.convertDeviceType
import com.tencent.devops.scheduler.pojo.tree.NetWorkTreeType.convertNetworkType
import com.tencent.devops.scheduler.pojo.tree.OsTreeType.convertOsType
import com.tencent.devops.scheduler.pojo.tree.ProxyTagNames.FlagOfGray
import com.tencent.devops.scheduler.pojo.tree.ProxyTagNames.FlagOfNetflowFree
import com.tencent.devops.scheduler.pojo.tree.ProxyTagNames.FlagOfNoWebProxy
import com.tencent.devops.scheduler.pojo.tree.ProxyTagNames.FlagOfOneSidedNetwork
import com.tencent.devops.scheduler.pojo.tree.ProxyTagNames.FlagOfPresentation
import com.tencent.devops.scheduler.pojo.tree.ProxyTagNames.FlagOfWebProxy
import com.tencent.devops.scheduler.schedule.ResourceFilter
import com.tencent.devops.scheduler.schedule.ScheduleContext
import com.tencent.devops.scheduler.schedule.TelecomDefines.TELECOM_OF_BGP
import com.tencent.devops.scheduler.schedule.TelecomDefines.TELECOM_OF_CHINA_TELECOM
import com.tencent.devops.scheduler.schedule.TelecomDefines.parseTelecomType
import com.tencent.devops.scheduler.schedule.profile.DecisionSource
import com.tencent.devops.scheduler.schedule.profile.SearchProfileBuilder
import com.tencent.devops.scheduler.schedule.profile.TreeSearchProfile
import com.tencent.devops.scheduler.util.IDParseTool
import com.tencent.devops.scheduler.util.XCommonUtil.parseCgsID
import org.jooq.DSLContext
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.stereotype.Component

@Component
class ProxyTreeFilter @Autowired constructor(
    private val metaMgr: MetaMgr,
    private val treeBuilder: TreeBuilder,
    private val proxyDao: ProxyDao,
    @Qualifier("cloudgameDslContext")
    private val cloudgameDslContext: DSLContext
) : ResourceFilter {
    override fun filter(context: ScheduleContext, inList: List<String>): List<String> {
        logger.info("ProxyTreeFilter filter, inList: $inList, proxyWhiteList: ${context.req.proxyWhiteList}")
        val backupZones = proxyDao.getBackupZones(cloudgameDslContext, context.proxyRegion)
        initRequestProfile(context, backupZones.isNotEmpty())
        context.searchProfile?.treeFinalExecuted = true
        context.req.proxyWhiteList?.takeIf { it.resourceList.isNotEmpty() }?.let { whiteList ->
            var newRegion: String? = null
            val result = whiteList.resourceList.map { resource ->
                val (region, proxyIp) = parseCgsID(resource)
                newRegion = region
                proxyIp
            }.toMutableList()

            newRegion?.takeIf { it.isNotEmpty() && it != context.proxyRegion }?.let {
                context.proxyRegion = it
            }

            context.searchProfile?.apply {
                treePrimaryExecuted = true
                treePrimaryOut = result.size
                treeFinalOut = result.size
                reasonCodes += "tree_stage:PROXY_WHITELIST"
                treeSearchProfile = TreeSearchProfile(
                    metaNames = metaMgr.load().map { it.name },
                    primarySearchPath = result,
                    primaryResultCount = result.size,
                    usedBackupZone = false,
                    finalTreeResultCount = result.size
                )
            }

            return result
        }

        // 代理树过滤
        val proxyList = treeFilter(context, inList)
        return proxyList
    }

    override fun name(): String {
        return "ProxyTreeFilter"
    }

    override fun init() {

    }

    override fun isUse(context: ScheduleContext): Boolean {
        return true
    }

    /**
     * 过滤器方法
     * 逻辑说明：
     * 1. 如果BGP选择不到，选择电信
     * 2. 如果免流选择不到，选择非免流
     * 3. 如果演示区域找不到，选择正式
     * @param ctx 上下文对象
     * @param in 输入参数
     * @return Pair<List<Any>?, Exception?> 返回代理列表和可能的错误
     */
    private fun treeFilter(context: ScheduleContext, inList: List<String>) : List<String> {

        // searchPath: [NJ1 1 中国电信 8 2 4 980083 0.11.4.19963 <nil>]
        /**
         * root
         * ├── zone1
         * │   ├── status1
         * │   │   ├── telecom1
         * │   │   │   └── 192.168.1.1
         * │   │   └── telecom2
         * │   │       └── 192.168.1.2
         * │   └── status2
         * └── zone2
         *     └── status3
         *         └── telecom3
         *             └── 192.168.2.1
         *
         */

        // 获取请求对象
        val req = context.req

        // 判断是否使用备用代理
        val isMainTelecom = parseTelecomType(req.telecom).second
        val isUseBySpeedTest = context.speedTestAllocProxyPolicy
        val isUseBackup = !isMainTelecom && !isUseBySpeedTest
        val backupZones = if (context.proxyRegion.isNotEmpty()) {
            proxyDao.getBackupZones(cloudgameDslContext, context.proxyRegion)
        } else {
            emptyList()
        }

        // 构建搜索路径
        val searchPath = makeSearchPath(context)

        logger.info("Search path: $searchPath")

        // 首次搜索主区域
        context.searchProfile?.treePrimaryExecuted = true
        var proxyList = searchProxy(searchPath)

        context.searchProfile?.treePrimaryOut = proxyList.size
        if (proxyList.isNotEmpty()) {
            logger.info("Main region search succeeded, proxy list: $proxyList, isUseBackup: $isUseBackup")
            if (isUseBackup) {
                context.backupProxyList = getBackupProxyList(searchPath)
            }
            updateTreeSearchProfile(
                context = context,
                searchPath = searchPath,
                primaryResultCount = proxyList.size,
                finalTreeResultCount = proxyList.size
            )
            context.searchProfile?.apply {
                treeFinalOut = proxyList.size
                reasonCodes += "tree_stage:PRIMARY_SUCCESS"
            }
            return proxyList
        }

        // 主区域没有Proxy，尝试备份区域
        if (context.proxyRegion.isNotEmpty()) {
            logger.info("Main region ${context.proxyRegion} has no available proxy, trying backup regions")
            for (backupZone in backupZones) {
                context.searchProfile?.treeBackupExecuted = true
                logger.info("Trying backup region ${backupZone.backupZoneId}, priority: ${backupZone.priority}")
                
                // 修改搜索路径中的区域为备份区域
                val backupSearchPath = searchPath.toMutableList().apply { 
                    this[0] = backupZone.backupZoneId 
                }
                
                // 在备份区域中搜索
                proxyList = searchProxy(backupSearchPath)
                context.searchProfile?.treeBackupOut = proxyList.size
                
                if (proxyList.isNotEmpty()) {
                    logger.info("Backup region ${backupZone.backupZoneId} search succeeded, proxy list: $proxyList")
                    // 更新上下文中的区域信息
                    context.proxyRegion = backupZone.backupZoneId
                    if (isUseBackup) {
                        context.backupProxyList = getBackupProxyList(backupSearchPath)
                    }
                    updateTreeSearchProfile(
                        context = context,
                        searchPath = searchPath,
                        primaryResultCount = 0,
                        usedBackupZone = true,
                        backupZoneId = backupZone.backupZoneId,
                        backupSearchPath = backupSearchPath,
                        backupResultCount = proxyList.size,
                        finalTreeResultCount = proxyList.size
                    )
                    context.searchProfile?.apply {
                        treeFinalOut = proxyList.size
                        reasonCodes += "tree_stage:BACKUP_ZONE_USED"
                    }
                    return proxyList
                }
                
                logger.info("Backup region ${backupZone.backupZoneId} has no available proxy, trying next backup region")
            }
            
            logger.info("All backup regions have no available proxy, trying degradation strategy")
        }

        // 免流搜索不到，选择非免流
        if (req.proxyTags.contains(FlagOfNetflowFree)) {
            context.searchProfile?.treeNetflowDegradeExecuted = true
            val modifiedPath = searchPath.toMutableList().apply { this[this.size - 1] = null }
            proxyList = searchProxy(modifiedPath)
            context.searchProfile?.treeNetflowDegradeOut = proxyList.size

            if (proxyList.isNotEmpty()) {
                logger.info("Non-netflow-free proxy list: $proxyList")
                if (isUseBackup) {
                    getBackupProxyList(searchPath)
                }
                updateTreeSearchProfile(
                    context = context,
                    searchPath = searchPath,
                    primaryResultCount = 0,
                    usedNetflowDegrade = true,
                    netflowDegradePath = modifiedPath,
                    netflowDegradeResultCount = proxyList.size,
                    finalTreeResultCount = proxyList.size
                )
                context.searchProfile?.apply {
                    treeFinalOut = proxyList.size
                    reasonCodes += "tree_stage:NETFLOW_DEGRADE_USED"
                }
                return proxyList
            }
        }

        // 演示区域没有，选择正式环境
        if (req.envTags.contains(FlagOfPresentation)) {
            context.searchProfile?.treePresentationDegradeExecuted = true
            val modifiedPath = searchPath.toMutableList().apply { this[1] = ProxyStatus.ONLINE.value }
            proxyList = searchProxy(modifiedPath)
            context.searchProfile?.treePresentationDegradeOut = proxyList.size

            if (proxyList.isNotEmpty()) {
                logger.debug("正式区域代理列表: $proxyList")
                if (isUseBackup) {
                    getBackupProxyList(searchPath)
                }
                updateTreeSearchProfile(
                    context = context,
                    searchPath = searchPath,
                    primaryResultCount = 0,
                    usedPresentationDegrade = true,
                    presentationDegradePath = modifiedPath,
                    presentationDegradeResultCount = proxyList.size,
                    finalTreeResultCount = proxyList.size
                )
                context.searchProfile?.apply {
                    treeFinalOut = proxyList.size
                    reasonCodes += "tree_stage:PRESENTATION_DEGRADE_USED"
                }
                return proxyList
            }
        }

        context.searchProfile?.apply {
            treeFinalOut = 0
            reasonCodes += "tree_stage:NO_MATCH"
        }
        updateTreeSearchProfile(
            context = context,
            searchPath = searchPath,
            primaryResultCount = 0,
            finalTreeResultCount = 0
        )
        logger.error("No proxy data found in configuration tree")
        return emptyList()
    }

    private fun initRequestProfile(context: ScheduleContext, hasBackupZones: Boolean) {
        val searchProfile = context.searchProfile ?: return
        if (searchProfile.requestProfile != null) return
        val requestProfile = SearchProfileBuilder.buildRequestProfile(
            req = context.req,
            decisionSource = DecisionSource.FRESH_ALLOC,
            hasBackupZones = hasBackupZones,
            speedTestAllocProxyPolicy = context.speedTestAllocProxyPolicy
        )
        searchProfile.requestProfile = requestProfile
        searchProfile.profileSignature = SearchProfileBuilder.buildProfileSignature(requestProfile)
    }

    private fun updateTreeSearchProfile(
        context: ScheduleContext,
        searchPath: List<Any?>,
        primaryResultCount: Int,
        usedBackupZone: Boolean = false,
        backupZoneId: String? = null,
        backupSearchPath: List<Any?> = emptyList(),
        backupResultCount: Int = 0,
        usedNetflowDegrade: Boolean = false,
        netflowDegradePath: List<Any?> = emptyList(),
        netflowDegradeResultCount: Int = 0,
        usedPresentationDegrade: Boolean = false,
        presentationDegradePath: List<Any?> = emptyList(),
        presentationDegradeResultCount: Int = 0,
        finalTreeResultCount: Int
    ) {
        context.searchProfile?.treeSearchProfile = TreeSearchProfile(
            metaNames = metaMgr.load().map { it.name },
            primarySearchPath = searchPath.map { it?.toString() },
            primaryResultCount = primaryResultCount,
            usedBackupZone = usedBackupZone,
            backupZoneId = backupZoneId,
            backupSearchPath = backupSearchPath.map { it?.toString() },
            backupResultCount = backupResultCount,
            usedNetflowDegrade = usedNetflowDegrade,
            netflowDegradePath = netflowDegradePath.map { it?.toString() },
            netflowDegradeResultCount = netflowDegradeResultCount,
            usedPresentationDegrade = usedPresentationDegrade,
            presentationDegradePath = presentationDegradePath.map { it?.toString() },
            presentationDegradeResultCount = presentationDegradeResultCount,
            finalTreeResultCount = finalTreeResultCount
        )
    }

    /**
     * 根据searchPath逐层搜索
     */
    fun searchProxy(searchPath: List<Any?>): List<String> {
        val treeNode = treeBuilder.getProxyTree()
        // 初始化当前节点为根节点
        var currentNode: TreeNode = treeNode

        // 遍历搜索路径
        for (path in searchPath) {
            // 如果当前节点是分支节点
            if (currentNode is TreeNode.BranchNode) {
                // 在子节点中查找匹配的节点
                val nextNode = currentNode.children[path] ?: currentNode.children["ANY"]
                if (nextNode != null) {
                    // 更新当前节点
                    currentNode = nextNode
                } else {
                    // 如果找不到匹配的节点，返回空列表
                    return emptyList()
                }
            } else {
                // 如果当前节点是叶子节点，且还有未遍历的路径，返回空列表
                return emptyList()
            }
        }

        // 如果最终节点是叶子节点，返回IP列表
        return if (currentNode is TreeNode.LeafNode) {
            currentNode.ips.toList()
        } else {
            // 如果最终节点不是叶子节点，返回空列表
            emptyList()
        }
    }

    private fun getBackupProxyList(searchPath: List<Any?>): List<String> {
        // 变更searchPath[2]的值为"电信"，并返回新的searchPath list
        val newSearchPath = searchPath.toMutableList()
        newSearchPath[2] = TELECOM_OF_CHINA_TELECOM
        return searchProxy(newSearchPath)
    }

    // 搜索路径构建
    private fun makeSearchPath(context: ScheduleContext): List<Any?> {
        val metas = metaMgr.load()
        logger.info("makeSearchPath: ${JsonUtil.toJson(context)}, metas: ${metas.size}")
        return metas.map { meta ->
            when (meta.name) {
                "zone" -> context.proxyRegion
                "status" -> handleStatus(context.req.envTags)
                "telecom" -> handleTelecom(context.req.telecom)
                "network_type" -> convertNetworkType(context.req.networkType)
                "os_type" -> convertOsType(context.req.clientType)
                "device_type" -> convertDeviceType(context.req.deviceType)
                "gameid" -> context.req.gameId
                "client_version" -> context.req.clientVersion
                "netflow_free" -> handleNetflowFree(context.req.proxyTags)
                "web_proxy" -> handleWebProxy(context.req.clientVersion)
                "single_sided_network" -> handleSingleSidedNetwork(context.req.proxyTags)
                else -> throw IllegalArgumentException("不支持的搜索路径[${meta.name}]")
            }
        }
    }

    // 状态处理（私有方法封装）
    private fun handleStatus(envTags: List<String>): Int {
        return if (envTags.contains(FlagOfPresentation)) {
            ProxyStatus.PRESENTATION.value
        } else if (envTags.contains(FlagOfGray)){
            ProxyStatus.GRAY.value
        } else {
            ProxyStatus.ONLINE.value
        }
    }

    // 运营商处理（使用扩展函数优化）
    private fun handleTelecom(telecom: String): String {
        return if (!parseTelecomType(telecom).second) {
            TELECOM_OF_BGP
        } else {
            telecom
        }
    }

    // 免流标签处理（空安全优化）
    private fun handleNetflowFree(proxyTags: List<String>): String? {
        return if (proxyTags.contains(FlagOfNetflowFree)) FlagOfNetflowFree else null
    }

    // 网页代理处理
    private fun handleWebProxy(clientVersion: String): String {
        return if (IDParseTool.isWebGame(clientVersion)) FlagOfWebProxy else FlagOfNoWebProxy
    }

    // 网页代理处理（使用when表达式）
    private fun handleSingleSidedNetwork(proxyTags: List<String>): String? {
        return if (proxyTags.contains(FlagOfOneSidedNetwork)) FlagOfOneSidedNetwork else null
    }

    companion object {
        private val logger = LoggerFactory.getLogger(ProxyTreeFilter::class.java)
    }
}