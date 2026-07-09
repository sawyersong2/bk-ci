package com.tencent.devops.scheduler.schedule

import com.tencent.devops.common.api.exception.ErrorCodeException
import com.tencent.devops.scheduler.constant.ErrorMessageCode
import com.tencent.devops.scheduler.pojo.DirectAllocRequest
import org.slf4j.LoggerFactory
import java.time.Duration
import java.time.Instant
import java.util.*
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.CopyOnWriteArrayList

/**
 * 策略管理器
 * @property policies 策略列表
 * @property filterMgrs 策略对应的过滤器管理器映射
 * @property filterMapMgrs 策略对应的过滤器映射表
 * @property allocMgrs 策略对应的分配器管理器映射
 */
class PolicyManager private constructor(
    private val policies: List<Policy>,
    private val filterMgrs: ConcurrentHashMap<Policy, FilterManager>,
    private val filterMapMgrs: ConcurrentHashMap<Policy, ConcurrentHashMap<String, FilterManager>>,
    private val allocMgrs: ConcurrentHashMap<Policy, AllocatorManager>
) {

    /**
     * 内部执行方法
     * @param context 执行上下文
     */
    fun innerRun(context: ScheduleContext): Any? {
        for (policy in policies) {
            val start = Instant.now()
            if (!policy.isUse(context)) continue

            filterMgrs[policy]?.let { context.filterMgr = it }
            filterMapMgrs[policy]?.forEach {
                context.filterMapMgr = ConcurrentHashMap<String, ListManager>().apply {
                    putAll(mapOf((it.key to it.value)))
                }
            }
            allocMgrs[policy]?.let { context.allocMgr = it }

            val result = policy.execute(context)

            logger.info("""
                Policy ${policy.name()} succeeded. 
                Result: ${result?.toString()?.take(200)}...
                Duration: ${Duration.between(start, Instant.now()).toMillis()}ms
            """.trimIndent())

            if (!result.isNull()) {
                return result
            }
        }

        throw ErrorCodeException(
            errorCode = ErrorMessageCode.SCHEDULE_ERROR,
            defaultMessage = "No resources found after executing all policies"
        )
    }

    // 扩展函数实现空检查
    private fun Any?.isNull(): Boolean = when {
        this == null -> true
        this is Optional<*> -> !isPresent
        this is Collection<*> -> isEmpty()
        else -> false
    }

    /**
     * 执行入口
     * @param directAllocRequest 请求对象
     */
    fun run(directAllocRequest: DirectAllocRequest): Any? {
        val context = ScheduleContext(req = directAllocRequest)
        return innerRun(context)
    }

    /**
     * 建造者模式实现
     */
    class Builder {
        private val policies = mutableListOf<Policy>()
        private val filterBuilders = mutableMapOf<Policy, FilterManager.Builder>()
        private val filterMapBuilders = mutableMapOf<Policy, MutableMap<String, FilterManager.Builder>>()
        private val allocBuilders = mutableMapOf<Policy, AllocatorManager.Builder>()

        /**
         * 添加策略
         * @param policy 策略实例
         * @param alloc 分配器实例（可选）
         * @param filters 过滤器列表（可选）
         */
        fun add(
            policy: Policy,
            alloc: Allocator? = null,
            vararg filters: ResourceFilter
        ): Builder {
            policies.add(policy)
            filterBuilders[policy] = FilterManager.Builder().apply {
                filters.forEach { addFilter(it) }
            }
            alloc?.let {
                allocBuilders[policy] = AllocatorManager.Builder().apply {
                    addAllocator(it)
                }
            }
            return this
        }

        /**
         * 添加过滤器映射
         */
        fun addFilterMap(
            policy: Policy,
            mapName: String,
            vararg filters: ResourceFilter
        ): Builder {
            val policyMap = filterMapBuilders.getOrPut(policy) { mutableMapOf() }
            val builder = policyMap.getOrPut(mapName) { FilterManager.Builder() }
            filters.forEach { builder.addFilter(it) }
            return this
        }


        /**
         * 构建策略管理器
         */
        fun build(): PolicyManager {
            // 初始化所有策略
            policies.forEach { it.init() }

            // 构建过滤器管理器
            val filterMgrs = ConcurrentHashMap(filterBuilders.mapValues { it.value.build() })

            // 构建分配器管理器
            val allocMgrs = ConcurrentHashMap(allocBuilders.mapValues { it.value.build() })

            // 构建过滤器映射表
            val filterMapMgrs = filterMapBuilders.mapValues { policyEntry ->
                ConcurrentHashMap<String, FilterManager>().apply {
                    putAll(policyEntry.value.mapValues { it.value.build() })
                }
            }.let { ConcurrentHashMap(it) }

            return PolicyManager(
                policies = policies.toList(),
                filterMgrs = filterMgrs,
                filterMapMgrs = filterMapMgrs,
                allocMgrs = allocMgrs
            )
        }
    }

    companion object {
        /**
         * 实现NewBuild工厂方法
         */
        fun newBuild(): Builder {
            return Builder()
        }

        private val logger = LoggerFactory.getLogger(Builder::class.java)
    }
}