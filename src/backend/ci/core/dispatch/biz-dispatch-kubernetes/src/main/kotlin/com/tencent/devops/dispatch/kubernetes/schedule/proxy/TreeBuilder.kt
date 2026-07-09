package com.tencent.devops.scheduler.schedule.proxy

import com.github.benmanes.caffeine.cache.Caffeine
import com.tencent.devops.scheduler.dao.ProxyDao
import com.tencent.devops.scheduler.pojo.ProxyStatus
import com.tencent.devops.scheduler.pojo.tree.DeviceTreeType
import com.tencent.devops.scheduler.pojo.tree.NetWorkTreeType
import com.tencent.devops.scheduler.pojo.tree.OsTreeType
import com.tencent.devops.scheduler.pojo.tree.ProxyTagNames
import com.tencent.devops.scheduler.schedule.TelecomDefines
import com.tencent.devops.scheduler.schedule.TelecomDefines.BIT_FLAG_OF_BGP
import com.tencent.devops.scheduler.schedule.TelecomDefines.BIT_FLAG_OF_CHINA_MOBILE
import com.tencent.devops.scheduler.schedule.TelecomDefines.BIT_FLAG_OF_CHINA_TELECOM
import com.tencent.devops.scheduler.schedule.TelecomDefines.BIT_FLAG_OF_CHINA_UNICOM
import org.jooq.DSLContext
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.stereotype.Component
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock

/**
 * 动态层级配置：通过Meta数据定义树形结构层级，便于扩展
 * 多分支支持：单个代理可能生成多个树路径（当有多个可选属性时）
 * 高效查询：树形结构便于快速匹配符合条件的代理
 * 属性组合：支持多种属性组合的复杂查询条件
 */
@Component
class TreeBuilder @Autowired constructor(
    @Qualifier("cloudgameDslContext")
    private val cloudgameDslContext: DSLContext,
    private val proxyDao: ProxyDao,
    private val metaMgr: MetaMgr
) {
    private val loadLock = ReentrantLock()

    private val idGenerator = AtomicLong(0)

    private val filterKey = listOf("中国电信", "中国联通", "中国移动", "BGP")

    private fun generateId(): Long = idGenerator.incrementAndGet()

    companion object {
        private val logger = LoggerFactory.getLogger(TreeBuilder::class.java)
        private const val PROXY_TREE_CACHE_KEY = "proxyTree"
    }

    // 使用Caffeine高性能缓存
    private val proxyTreeCache = Caffeine.newBuilder()
        .maximumSize(2000)
        .expireAfterWrite(10, TimeUnit.MINUTES)
        .refreshAfterWrite(10, TimeUnit.MINUTES)
        .recordStats() // 记录缓存统计信息
        .build<String, ProxyTreeData> {
            loadProxyTreeData()
        }

    // 缓存最后刷新时间
    @Volatile
    private var lastRefreshTime: Long = 0L

    /**
     * 获取代理树（优先从缓存读取）
     */
    fun getProxyTree(): TreeNode {
        return proxyTreeCache.get(PROXY_TREE_CACHE_KEY).tree
    }

    /**
     * 获取代理树（优先从缓存读取）
     */
    fun getProxyTreeTelecomMap(): Map<Int, Set<String>> {
        return proxyTreeCache.get(PROXY_TREE_CACHE_KEY).telecomMap
    }

    /**
     * 刷新代理树缓存（供外部接口调用）
     * 当 t_proxy 表数据更新时调用此方法刷新缓存
     */
    fun refreshProxyTreeCache() {
        logger.info("Refreshing proxy tree cache, triggered by external update")
        try {
            proxyTreeCache.invalidate(PROXY_TREE_CACHE_KEY)
            // 立即重新加载缓存
            proxyTreeCache.get(PROXY_TREE_CACHE_KEY)
            lastRefreshTime = System.currentTimeMillis()
            logger.info("Proxy tree cache refreshed successfully at $lastRefreshTime")
        } catch (e: Exception) {
            logger.error("Failed to refresh proxy tree cache: ${e.message}", e)
            throw e
        }
    }

    /**
     * 使代理树缓存失效（仅失效，不立即重新加载）
     */
    fun invalidateProxyTreeCache() {
        logger.info("Invalidating proxy tree cache")
        proxyTreeCache.invalidate(PROXY_TREE_CACHE_KEY)
    }

    /**
     * 获取缓存统计信息
     */
    fun getCacheStats(): ProxyTreeCacheStats {
        val stats = proxyTreeCache.stats()
        return ProxyTreeCacheStats(
            hitCount = stats.hitCount(),
            missCount = stats.missCount(),
            hitRate = stats.hitRate(),
            evictionCount = stats.evictionCount(),
            lastRefreshTime = lastRefreshTime,
            estimatedSize = proxyTreeCache.estimatedSize()
        )
    }

    fun loadProxyTreeData(): ProxyTreeData {
        loadLock.withLock {
            val localTelecomMap = ConcurrentHashMap<Int, MutableSet<String>>()
            localTelecomMap[BIT_FLAG_OF_BGP] = mutableSetOf()
            localTelecomMap[BIT_FLAG_OF_CHINA_TELECOM] = mutableSetOf()
            localTelecomMap[BIT_FLAG_OF_CHINA_UNICOM] = mutableSetOf()
            localTelecomMap[BIT_FLAG_OF_CHINA_MOBILE] = mutableSetOf()

            val proxyMap = convert(proxyDao.getProxyData(cloudgameDslContext))
            val tree = buildTree(proxyMap.values.toList(), localTelecomMap)

            return ProxyTreeData(tree, localTelecomMap)
        }
    }

    private fun convert(proxyTableDataList: List<ProxyTableData>): Map<String, ProxyTableData> {
        return proxyTableDataList.mapNotNull { item ->
            when (item.groupStatus) {
                0 -> {
                    logger.debug("跳过无效组状态: ${item.groupStatus}")
                    null
                }

                2 -> handleGrayStatus(item)
                1 -> handleActiveStatus(item)
                else -> {
                    logger.warn("未知代理组状态: ${item.groupStatus}")
                    null
                }
            }?.also {
                it.tagsMap = parseTags(it.tags)
            }
        }.associateBy { it.innerIp }
    }

    // 处理灰度状态（私有方法封装）
    private fun handleGrayStatus(item: ProxyTableData): ProxyTableData {
        return item.apply { finalStatus = ProxyStatus.GRAY.value }
    }

    // 处理活跃状态（使用when表达式优化）
    private fun handleActiveStatus(item: ProxyTableData): ProxyTableData? {
        return when (item.proxyStatus) {
            1 -> handleOnlineStatus(item)
            2 -> item.apply { finalStatus = ProxyStatus.GRAY.value }
            else -> {
                logger.warn("未知代理状态: ${item.proxyStatus}")
                null
            }
        }
    }

    // 处理在线状态（细化状态判断）
    private fun handleOnlineStatus(item: ProxyTableData): ProxyTableData {
        return item.apply {
            finalStatus = when (status2) {
                2 -> ProxyStatus.PRESENTATION.value
                else -> ProxyStatus.ONLINE.value
            }
        }
    }

    // 标签解析（使用扩展函数优化）
    private fun parseTags(tags: String): Set<String> {
        return tags.splitToSequence(',')
            .map { it.trim() }
            .filter { it.isNotEmpty() }
            .toSet()
    }

    fun buildTree(
        proxies: List<ProxyTableData>,
        localTelecomMap: ConcurrentHashMap<Int, MutableSet<String>>
    ): TreeNode {
        val root = createRootNode()
        proxies.forEach { proxy ->
            var currentNodes = mutableListOf<TreeNode>()
            currentNodes.add(root)

            val metas = metaMgr.load()
            metas.forEach { meta ->
                currentNodes = when (meta.name) {
                    "zone" -> handleZone(proxy, currentNodes, meta)
                    "status" -> handleStatus(proxy, currentNodes, meta)
                    "telecom" -> handleTelecom(proxy, currentNodes, meta, localTelecomMap)
                    "network_type" -> handleNetwork(proxy.networkMask, currentNodes, meta)
                    "os_type" -> handleOsType(proxy.clientTypeMask, currentNodes, meta)
                    "device_type" -> handleDeviceType(proxy.deviceTypeMask, currentNodes, meta)
                    "gameid" -> handleGame(proxy.tagsMap, currentNodes, meta)
                    "client_version" -> handleClientVersion(proxy.tagsMap, currentNodes, meta)
                    "netflow_free" -> handleNetflowFree(proxy.tagsMap, currentNodes, meta)
                    "web_proxy" -> handleWebProxy(proxy.tagsMap, currentNodes, meta)
                    "single_sided_network" -> handleSingleSidedNetwork(proxy.tagsMap, currentNodes, meta)
                    else -> throw IllegalArgumentException("Unsupported tree level: ${meta.name}")
                }
            }

            currentNodes.forEach { node ->
                if (node is TreeNode.LeafNode) {
                    node.ips.add(proxy.innerIp)
                }
            }
        }

        return root
    }

    private fun createRootNode(): TreeNode.BranchNode {
        return TreeNode.BranchNode(
            id = generateId(),
            name = "root",
            value = null,
            meta = null,
            children = mutableMapOf()
        )
    }

    private fun handleZone(
        proxy: ProxyTableData,
        nodes: MutableList<TreeNode>,
        meta: Meta
    ): MutableList<TreeNode> {
        return addChildNodes(nodes, listOf(proxy.zone), meta)
    }

    private fun handleStatus(
        proxy: ProxyTableData,
        nodes: MutableList<TreeNode>,
        meta: Meta
    ): MutableList<TreeNode> {
        return addChildNodes(nodes, listOf(proxy.finalStatus ?: 0), meta)
    }

    private fun handleTelecom(
        proxy: ProxyTableData,
        nodes: MutableList<TreeNode>,
        meta: Meta,
        localTelecomMap: ConcurrentHashMap<Int, MutableSet<String>>
    ): MutableList<TreeNode> {
        val tagsMap = proxy.tagsMap!!
        val telecoms = when {
            "中国电信" in tagsMap && "中国联通" in tagsMap && "中国移动" in tagsMap -> filterKey
            else -> tagsMap.filter { it in filterKey }
        }
        telecoms.forEach {
            localTelecomMap[TelecomDefines.parseTelecomType(it).first]?.add(proxy.innerIp)
        }
        require(telecoms.isNotEmpty()) { "No telecom found in tags: ${proxy.tagsMap}" }
        return addChildNodes(nodes, telecoms, meta)
    }

    private fun handleNetwork(
        networkMask: Int,
        nodes: MutableList<TreeNode>,
        meta: Meta
    ): MutableList<TreeNode> {
        val params = buildList {
            if (networkMask == 0) {
                add(NetWorkTreeType.ANY)
                add(NetWorkTreeType.WIFI)
                add(NetWorkTreeType.MOBILE_4G)
                add(NetWorkTreeType.MOBILE_5G)
                add(NetWorkTreeType.CABEL)
            }
            if (networkMask and NetWorkTreeType.WIFI != 0) add(NetWorkTreeType.WIFI)
            if (networkMask and NetWorkTreeType.MOBILE_4G != 0) add(NetWorkTreeType.MOBILE_4G)
            if (networkMask and NetWorkTreeType.MOBILE_5G != 0) add(NetWorkTreeType.MOBILE_5G)
            if (networkMask and NetWorkTreeType.CABEL != 0) add(NetWorkTreeType.CABEL)
        }

        require(params.isNotEmpty()) { "No network type found in networkMask: $networkMask" }
        return addChildNodes(nodes, params, meta)
    }

    private fun handleOsType(
        clientTypeMask: Int,
        nodes: MutableList<TreeNode>,
        meta: Meta
    ): MutableList<TreeNode> {
        val params = buildList {
            if (clientTypeMask == 0) {
                add(OsTreeType.ANY)
                add(OsTreeType.IOS)
                add(OsTreeType.ANDROID)
                add(OsTreeType.WINDOWS)
                add(OsTreeType.MACOS)
                add(OsTreeType.ANDROID_TV)
                add(OsTreeType.BROWSER_PC)
                add(OsTreeType.BROWSER_H5)
            }
            if (clientTypeMask and OsTreeType.IOS != 0) add(OsTreeType.IOS)
            if (clientTypeMask and OsTreeType.ANDROID != 0) add(OsTreeType.ANDROID)
            if (clientTypeMask and OsTreeType.WINDOWS != 0) add(OsTreeType.WINDOWS)
            if (clientTypeMask and OsTreeType.MACOS != 0) add(OsTreeType.MACOS)
            if (clientTypeMask and OsTreeType.ANDROID_TV != 0) add(OsTreeType.ANDROID_TV)
            if (clientTypeMask and OsTreeType.BROWSER_PC != 0) add(OsTreeType.BROWSER_PC)
            if (clientTypeMask and OsTreeType.BROWSER_H5 != 0) add(OsTreeType.BROWSER_H5)
        }

        require(params.isNotEmpty()) { "No os type found in clientTypeMask: $clientTypeMask" }
        return addChildNodes(nodes, params, meta)
    }

    private fun handleDeviceType(
        deviceTypeMask: Int,
        nodes: MutableList<TreeNode>,
        meta: Meta
    ): MutableList<TreeNode> {
        val params = buildList {
            if (deviceTypeMask == 0) {
                add(DeviceTreeType.ANY)
                add(DeviceTreeType.UNKNOWN)
                add(DeviceTreeType.DESKTOP)
                add(DeviceTreeType.LAPTOP)
                add(DeviceTreeType.PHONE)
                add(DeviceTreeType.PAD)
                add(DeviceTreeType.STB)
                add(DeviceTreeType.TV)
            }
            if (deviceTypeMask and DeviceTreeType.UNKNOWN != 0) add(DeviceTreeType.UNKNOWN)
            if (deviceTypeMask and DeviceTreeType.DESKTOP != 0) add(DeviceTreeType.DESKTOP)
            if (deviceTypeMask and DeviceTreeType.LAPTOP != 0) add(DeviceTreeType.LAPTOP)
            if (deviceTypeMask and DeviceTreeType.PHONE != 0) add(DeviceTreeType.PHONE)
            if (deviceTypeMask and DeviceTreeType.PAD != 0) add(DeviceTreeType.PAD)
            if (deviceTypeMask and DeviceTreeType.STB != 0) add(DeviceTreeType.STB)
            if (deviceTypeMask and DeviceTreeType.TV != 0) add(DeviceTreeType.TV)
        }

        require(params.isNotEmpty()) { "No device type found in deviceTypeMask: $deviceTypeMask" }
        return addChildNodes(nodes, params, meta)
    }

    private fun handleGame(
        tagsMap: Set<String>?,
        nodes: MutableList<TreeNode>,
        meta: Meta
    ): MutableList<TreeNode> {
        val params = tagsMap!!.asSequence()
            .filter { it.startsWith(ProxyTagNames.FlagOfGameId) }
            .map { it.removePrefix(ProxyTagNames.FlagOfGameId) }
            .toList()

        if (params.isEmpty()) {
            return addChildNodes(nodes, listOf("ANY"), meta)
        }
        return addChildNodes(nodes, params, meta)
    }

    private fun handleClientVersion(
        tagsMap: Set<String>?,
        nodes: MutableList<TreeNode>,
        meta: Meta
    ): MutableList<TreeNode> {
        val params = tagsMap!!.asSequence()
            .filter { it.startsWith(ProxyTagNames.FlagOfClientVersion) }
            .map { it.removePrefix(ProxyTagNames.FlagOfClientVersion) }
            .toList()

        if (params.isEmpty()) {
            return addChildNodes(nodes, listOf("ANY"), meta)
        }

        return addChildNodes(nodes, params, meta)
    }

    private fun handleNetflowFree(
        tagsMap: Set<String>?,
        nodes: MutableList<TreeNode>,
        meta: Meta
    ): MutableList<TreeNode> {
        val params = tagsMap!!.asSequence()
            .filter { it == ProxyTagNames.FlagOfNetflowFree }
            .toList()

        if (params.isEmpty()) {
            return addChildNodes(nodes, listOf("ANY"), meta)
        }
        return addChildNodes(nodes, params, meta)
    }

    private fun handleWebProxy(
        tagsMap: Set<String>?,
        nodes: MutableList<TreeNode>,
        meta: Meta
    ): MutableList<TreeNode> {
        val params = buildList {
            if (tagsMap!!.contains(ProxyTagNames.FlagOfWebProxy)) add(ProxyTagNames.FlagOfWebProxy)
            if (tagsMap.contains(ProxyTagNames.FlagOfNoWebProxy)) add(ProxyTagNames.FlagOfNoWebProxy)
            if (isEmpty()) add(ProxyTagNames.FlagOfUnknownProxy)
        }

        return addChildNodes(nodes, params, meta)
    }

    private fun handleSingleSidedNetwork(
        tagsMap: Set<String>?,
        nodes: MutableList<TreeNode>,
        meta: Meta
    ): MutableList<TreeNode> {
        val params = tagsMap!!.asSequence()
            .filter { it == ProxyTagNames.FlagOfOneSidedNetwork }
            .toList()

        if (params.isEmpty()) {
            return addChildNodes(nodes, listOf("ANY"), meta)
        }
        logger.info("single sided network: $params")
        return addChildNodes(nodes, params, meta)
    }

    private fun addChildNodes(
        parentNodes: List<TreeNode>,
        values: List<Any>,
        meta: Meta
    ): MutableList<TreeNode> {
        return parentNodes.flatMap { parent ->
            when (parent) {
                is TreeNode.BranchNode -> {
                    values.map { value ->
                        val key = getKey(value, meta)
                        parent.children.getOrPut(key) {
                            if (meta.child == null) {
                                TreeNode.LeafNode(
                                    id = generateId(),
                                    name = null,
                                    value = value,
                                    meta = meta
                                )
                            } else {
                                TreeNode.BranchNode(
                                    id = generateId(),
                                    name = null,
                                    value = value,
                                    meta = meta,
                                    children = mutableMapOf()
                                )
                            }
                        }
                    }
                }

                is TreeNode.LeafNode -> emptyList()
            }
        }.toMutableList()
    }

    private fun getKey(value: Any, meta: Meta): Any {
        return if (meta.flags.isUseKey()) value else generateId()
    }

    private fun createChildNode(value: Any, meta: Meta): TreeNode {
        return if (meta.child == null) {
            TreeNode.LeafNode(
                id = generateId(),
                name = null,
                value = value,
                meta = meta
            )
        } else {
            TreeNode.BranchNode(
                id = generateId(),
                name = null,
                value = value,
                meta = meta,
                children = mutableMapOf()
            )
        }
    }
}

sealed class TreeNode {
    abstract val id: Any
    abstract val name: String?
    abstract val value: Any?
    abstract val meta: Meta?
    abstract val children: MutableMap<Any, TreeNode>
    abstract val defaultChild: TreeNode?

    data class BranchNode(
        override val id: Any,
        override val name: String?,
        override val value: Any?,
        override val meta: Meta?,
        override val children: MutableMap<Any, TreeNode> = mutableMapOf(),
        override var defaultChild: TreeNode? = null
    ) : TreeNode()

    data class LeafNode(
        override val id: Any,
        override val name: String?,
        override val value: Any?,
        override val meta: Meta?,
        val ips: MutableSet<String> = mutableSetOf()
    ) : TreeNode() {
        override val children: MutableMap<Any, TreeNode> get() = mutableMapOf()
        override val defaultChild: TreeNode? get() = null
    }
}

data class Meta(
    val id: Int,
    val name: String,
    val dataType: NodeDataType,
    val level: Int,
    val flags: NodeFlag,
    val searchFiled: String,
    var child: Meta? = null,
    val parent: Meta? = null,
    val validValue: Map<String, String> = emptyMap(),
    val loadFiled: String? = null,
    val loadFieldDataType: Int? = null
)

// 树节点数据类型枚举
enum class NodeDataType(val value: Int) {
    NOTHING(0),                 // 无意义类型
    INT64(1),                   // 64位整型
    UINT64(2),                  // 无符号64位整型
    STRING(3),                  // 字符串精确匹配
    STRING_EQUAL_FOLD(4),       // 字符串不区分大小写匹配
    STRING_PREFIX(5),           // 字符串前缀匹配
    STRING_SUFFIX(6),           // 字符串后缀匹配
    REGEX(7),                   // 正则表达式匹配
    INT64_LIST(8),              // 逗号分隔的Int64列表（仅数据源配置）
    UINT64_LIST(9),             // 逗号分隔的UInt64列表（仅数据源配置）
    CSV(10);                    // CSV格式字符串（仅数据源配置）

    companion object {
        fun fromValue(value: Int) = values().firstOrNull { it.value == value }
            ?: throw IllegalArgumentException("Invalid NodeDataType value: $value")
    }
}

// 树节点标志位（使用位运算封装）
enum class NodeFlag(val value: Int) {
    NOTHING(0),
    DEFAULT(1),
    USE_KEY(2),
    USE_BOTH(3);

    // 扩展函数实现位操作
    fun isUseKey() = (this.value and USE_KEY.value) != 0
    fun isDefaultNode() = (this.value and DEFAULT.value) != 0

    companion object {
        fun fromValue(value: Int) = NodeFlag.values().firstOrNull { it.value == value }
            ?: throw IllegalArgumentException("Invalid NodeFlag value: $value")
    }
}

data class ProxyTableData(
    val proxyId: Int,
    val zone: String,
    val tags: String,  // 保持原始字符串类型
    val groupName: String,
    val endpoint: String,
    val cdsEndpoint: String = "",
    val innerIp: String,
    val proxyStatus: Int,
    val version: String,
    val cvmInstanceId: String?,
    val networkMask: Int,
    val clientTypeMask: Int,
    val deviceTypeMask: Int,
    val groupStatus: Int,
    val telecomEndpoint: String,
    val unicomEndpoint: String,
    val mobileEndpoint: String,
    val bgpEndpoint: String,
    val telecomLinkType: Int,
    val unicomLinkType: Int,
    val mobileLinkType: Int,
    val bgpLinkType: Int,
    val status2: Int,
    val bandwidthLimit: Long,
    var finalStatus: Int? = 0,
    var tagsMap: Set<String>? = emptySet()
)

/**
 * 代理树缓存统计信息
 */
data class ProxyTreeCacheStats(
    val hitCount: Long,           // 缓存命中次数
    val missCount: Long,          // 缓存未命中次数
    val hitRate: Double,          // 缓存命中率
    val evictionCount: Long,      // 缓存驱逐次数
    val lastRefreshTime: Long,    // 最后刷新时间戳
    val estimatedSize: Long       // 缓存估计大小
)

data class ProxyTreeData(
    val tree: TreeNode,
    val telecomMap: Map<Int, Set<String>>
)