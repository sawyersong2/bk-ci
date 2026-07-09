package com.tencent.devops.scheduler.schedule

import com.tencent.devops.common.api.exception.ErrorCodeException
import com.tencent.devops.common.api.util.JsonUtil
import com.tencent.devops.scheduler.constant.ErrorMessageCode
import com.tencent.devops.scheduler.pojo.proxy.AllocProxyResponse
import org.slf4j.LoggerFactory
import java.util.concurrent.ConcurrentHashMap

class AllocatorManager private constructor(
    @Volatile private var allocator: Allocator?
): SingleManager {
    companion object {
        private const val LOCK_TIMEOUT_MS = 5000L
        private val logger = LoggerFactory.getLogger(AllocatorManager::class.java)
    }

    override fun run(scheduleContext: ScheduleContext, inList: List<String>): Any {
        val currentAllocator = allocator ?: return Pair(null, null)

        return try {
            currentAllocator.alloc(scheduleContext, inList).also {
                logger.info("Allocator ${currentAllocator.name()} executed successfully")
            }
        } catch (e: Exception) {
            logger.error("Allocator ${currentAllocator.name()} failed.", e)
            throw ErrorCodeException(
                errorCode = ErrorMessageCode.SCHEDULE_ERROR,
                defaultMessage = "Allocator ${currentAllocator.name()} failed. ${e.message}"
            )
        }
    }

    /**
     * 分配器建造者（线程安全实现）
     */
    class Builder {
        private val allocators = ConcurrentHashMap<String, Allocator>()

        /**
         * 添加分配器
         * @param allocator 分配器实例
         */
        fun addAllocator(allocator: Allocator): Builder {
            allocators[allocator.name()] = allocator.apply { init() }
            return this
        }

        /**
         * 构建分配器管理器
         */
        fun build(): AllocatorManager {
            return AllocatorManager(allocators.values.firstOrNull())
        }
    }
}