package com.tencent.devops.scheduler.schedule

import org.slf4j.LoggerFactory
import java.util.concurrent.CopyOnWriteArrayList

class FilterManager private constructor(
    private val filters: List<ResourceFilter>
) : ListManager {
    /**
     * 执行所有过滤器
     */
    override fun run(context: ScheduleContext, inList: List<String>): List<String> {
        var currentList = inList
        filters.forEach { filter ->
            if (!filter.isUse(context)) return@forEach

            val filteredList = filter.filter(context, currentList)
            currentList = filteredList
            if (currentList.isEmpty()) {
                logger.warn("Filter ${filter.name()} returned empty list")
            }
        }
        return currentList
    }

    /**
     * 根据名称执行单个过滤器
     */
    override fun runOneByName(name: String, context: ScheduleContext, inList: List<String>): List<String>{
        val filter = filters.find { it.name() == name } ?: return inList
        return executeSingleFilter(filter, context, inList)
    }

    /**
     * 根据类型执行单个过滤器
     */
    override fun runOneByType(type: Class<*>, context: ScheduleContext, inList: List<String>): List<String> {
        val filter = filters.find { it::class == type } ?: return inList
        return executeSingleFilter(filter, context, inList)
    }

    private fun executeSingleFilter(
        filter: ResourceFilter, context: ScheduleContext,
        inList: List<String>
    ): List<String> {
        if (!filter.isUse(context)) return inList

        val result = filter.filter(context, inList)
        logger.debug("Filter ${filter.name()} executed successfully")
        return result
    }

    /**
     * 过滤器建造者（线程安全实现）
     */
    class Builder {
        private val filters = CopyOnWriteArrayList<ResourceFilter>()

        /**
         * 添加过滤器
         * @param filter 过滤器实例
         */
        fun addFilter(filter: ResourceFilter): Builder {
            filters.add(filter)
            return this
        }

        /**
         * 构建过滤器管理器
         */
        fun build(): FilterManager {
            // 初始化所有过滤器
            filters.forEach { it.init() }
            return FilterManager(filters.toList())
        }
    }

    companion object {
        private val logger = LoggerFactory.getLogger(FilterManager::class.java)
    }
}