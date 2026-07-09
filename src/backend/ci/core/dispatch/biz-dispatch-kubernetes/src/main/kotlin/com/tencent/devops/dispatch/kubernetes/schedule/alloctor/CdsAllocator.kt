package com.tencent.devops.scheduler.schedule.alloctor

import com.tencent.devops.common.api.exception.ErrorCodeException
import com.tencent.devops.common.api.util.JsonUtil
import com.tencent.devops.common.redis.CommonRedisKey
import com.tencent.devops.common.redis.RedisLock
import com.tencent.devops.common.redis.RedisOperation
import com.tencent.devops.model.cloudgame.tables.records.TCdsInfoRecord
import com.tencent.devops.scheduler.constant.ErrorMessageCode
import com.tencent.devops.scheduler.dao.CdsServerDao
import com.tencent.devops.scheduler.pojo.DirectAllocResponse
import com.tencent.devops.scheduler.pojo.ResourceAllocation
import com.tencent.devops.scheduler.pojo.ResourceResult
import com.tencent.devops.scheduler.pojo.SessionInstance
import com.tencent.devops.scheduler.schedule.Allocator
import com.tencent.devops.scheduler.schedule.ScheduleContext
import com.tencent.devops.scheduler.util.CpuGroup
import com.tencent.devops.scheduler.util.EncoderUtils
import com.tencent.devops.scheduler.util.IDParseTool
import org.jooq.DSLContext
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.data.redis.core.DefaultTypedTuple
import org.springframework.data.redis.core.ZSetOperations
import org.springframework.stereotype.Component
import java.time.Instant
import java.util.concurrent.atomic.AtomicInteger
import kotlin.math.abs
import kotlin.math.ceil

/**
 * 基于cdsId的资源分配器
 * 用于根据cdsId进行资源分配，同时保留对nodeIp的兼容性
 */
@Component
class CdsAllocator @Autowired constructor(
    private val redisOperation: RedisOperation,
    @Qualifier("cloudgameDslContext")
    private val cloudgameDslContext: DSLContext,
    private val cdsServerDao: CdsServerDao
) : Allocator {

    override fun alloc(context: ScheduleContext, inList: List<String>): DirectAllocResponse {
        logger.info("CdsAllocator alloc context: $context, inList: $inList")
        val cdsWithNuma = parseCdsNuma(inList)
        inList.forEach {
            // 根据cdsId加分布式锁
            val redisLock = RedisLock(redisOperation, CDS_REDIS_LOCK_PREFIX + it, 3600L)
            try {
                val lockSuccess = redisLock.tryLock()
                if (lockSuccess) {
                    /*val cdsWithNumaResourceList = batchFetchResources(context, listOf(it))
                    if (cdsWithNumaResourceList.isEmpty()) {
                        return@forEach
                    }*/

                    val (success, unFoundReasons) = searchEachCdsNuma(context, it)
                    if (success) {
                        return DirectAllocResponse(
                            code = 0,
                            message = "success",
                            resource = context.allocResult!!,
                            searchProfile = context.searchProfile
                        )
                    } else {
                        throw ErrorCodeException(
                            errorCode = ErrorMessageCode.SCHEDULE_ERROR,
                            defaultMessage = "Allocate resource failed, ${JsonUtil.toJson(unFoundReasons)}"
                        )
                    }
                }
            } finally {
                redisLock.unlock()
            }
        }

        throw ErrorCodeException(
            errorCode = ErrorMessageCode.SCHEDULE_ERROR,
            defaultMessage = "Allocate resource failed, no resource found, ${JsonUtil.toJson(cdsWithNuma)}"
        )
    }

    override fun name(): String {
        return "CdsAllocator"
    }

    override fun init() {
    }

    override fun isUse(context: ScheduleContext): Boolean {
        return true
    }

    fun searchEachCdsNuma(
        context: ScheduleContext,
        cdsId: String
    ): Pair<Boolean, List<String>> {
        logger.info("searchEachCdsNuma context: $context, cdsId: $cdsId")
        // val gameCfg = context.gameConfig
        val proxyAllocRes = context.allocProxyResponse
        val unFoundReasons = mutableListOf<String>()


            /*val (selectedGpu, maxGpu, selectedEncoder, maxEncoder) = resource.allocGpuForBalance(
                gameCfg?.resource?.gpu ?: 0,
                gameCfg?.resource?.encoder ?: 0
            )

            logger.info("searchEachCdsNuma resource: $resource, selectedGpu: $selectedGpu, maxGpu: $maxGpu, selectedEncoder: $selectedEncoder, maxEncoder: $maxEncoder")

            if (selectedGpu.isEmpty()) {
                logAndRecord(unFoundReasons, "selectedGpuID empty", resource)
                return@forEach
            }

            if (gameCfg?.resource?.encoder!! > 0 && selectedEncoder.isEmpty()) {
                logAndRecord(unFoundReasons, "selectedEncoderID empty", resource)
                return@forEach
            }

            val (cpuList, coresLeft) = resource.allocCpu(CommonRedisKey.DefaultCPUAllocedValue, gameCfg.resource.cpu)
            if (gameCfg.resource.cpu > 0 && cpuList.isEmpty()) {
                logAndRecord(unFoundReasons, "AllocCPU empty", resource)
                return@forEach
            }*/

            val cdsServerRecord = cdsServerDao.getCdsServerById(cloudgameDslContext, cdsId)

            // User operations
            setUser(
                context = context,
                cdsInfoRecord = cdsServerRecord,
                numaIdx = 0
            )

            // Add resource allocation commands
            /*addCommandsToPipeline(
                context = context,
                cdsId = resource.cdsId,
                numa = resource.numaIdx,
                selectedGpuIds = listOf(selectedGpu),
                cpuList = cpuList,
                selectedEncoderIds = listOf(selectedEncoder),
                maxGpuValue = maxGpu,
                coresLeft = coresLeft,
                maxEncoder = maxEncoder
            )
*/
            val allocResult = ResourceResult(
                cgsIp = cdsServerRecord?.innerIp ?: "",
                cdsId = cdsId,
                numa = 0,
                gpus = emptyList(),
                encoders = emptyList(),
                cpus = emptyList(),
                proxyIp = proxyAllocRes?.defaultProxy?.innerIp ?: "",
                proxyEndpoint = proxyAllocRes?.defaultProxy?.endPoint ?: "",
                cdsEndpoint = proxyAllocRes?.defaultProxy?.cdsEndpoint ?: "",
                proxyVersion = proxyAllocRes?.defaultProxy?.proxyVersion ?: "",
                proxyLinktype = proxyAllocRes?.defaultProxy?.linkType?.value ?: 0,
//                useGpu = gameCfg.resource.gpu,
//                useEncoder = gameCfg.resource.encoder,
//                useCpuCores = gameCfg.resource.cpu,
                resId = getResPoolId(context),
                cgsVersion = cdsServerRecord?.version ?: "",
                cgsGroupId = 0
            )

            context.allocResult = allocResult
            return Pair(true, unFoundReasons)
    }

    private fun logAndRecord(reasons: MutableList<String>, reason: String, resource: CdsWithNumaResource) {
        logger.error("Resource check failed: $reason at ${resource.cdsId}:${resource.numaIdx}")
        reasons.add(reason)
    }

    private fun parseCdsNuma(input: List<String>): List<CdsWithNuma> {
        return input.mapNotNull {
            IDParseTool.parseCdsNuma(it)?.let { (cdsId, numaIdx) ->
                CdsWithNuma(cdsId = cdsId, numaIdx = numaIdx)
            }
        }
    }

    fun batchFetchResources(
        context: ScheduleContext,
        cdsList: List<CdsWithNuma>
    ): List<CdsWithNumaResource> {
        val gameCfg = context.gameConfig
        val requireGpu = gameCfg?.resource?.gpu ?: 0
        val requireCpu = gameCfg?.resource?.cpu ?: 0
        val requireEncoder = gameCfg?.resource?.encoder ?: 0
        logger.info("Batch fetch resources: $cdsList requireGpu: $requireGpu requireCpu: $requireCpu requireEncoder: $requireEncoder")
        val responses = buildCommandPipeline(cdsList, requireCpu, requireEncoder)
        logger.info("Batch fetch resources: $responses")
        return processPipelineResults(responses, cdsList, requireGpu, requireCpu, requireEncoder)
    }

    private fun buildCommandPipeline(
        cdsList: List<CdsWithNuma>,
        requireCpu: Int,
        requireEncoder: Int
    ): Map<String, List<Set<ZSetOperations.TypedTuple<String>>>> {
        val responses = mutableMapOf<String, List<Set<ZSetOperations.TypedTuple<String>>>>()

        cdsList.forEach { (cdsId, numa) ->
            val keys = mutableListOf<Set<ZSetOperations.TypedTuple<String>>>().apply {
                add(
                    redisOperation.zrangeByScoreWithScores(
                        CommonRedisKey.redisKeyOfCgsGpuByCdsId(cdsId, numa),
                        0.0,
                        0xffffffff.toDouble()
                    ) ?: emptySet()
                )
                if (requireCpu > 0) add(
                    redisOperation.zrangeByScoreWithScores(
                        CommonRedisKey.redisKeyOfCgsCPUByCdsId(cdsId, numa),
                        0.0,
                        0xffffffff.toDouble()
                    ) ?: emptySet()
                )
                if (requireEncoder > 0) add(
                    redisOperation.zrangeByScoreWithScores(
                        CommonRedisKey.redisKeyOfCgsEncoderByCdsId(cdsId, numa),
                        0.0,
                        0xffffffff.toDouble()
                    ) ?: emptySet()
                )
            }
            responses["$cdsId:$numa"] = keys
        }

        return responses
    }

    private fun processPipelineResults(
        responses: Map<String, List<Set<ZSetOperations.TypedTuple<String>>>>,
        cdsList: List<CdsWithNuma>,
        requireGpu: Int,
        requireCpu: Int,
        requireEncoder: Int
    ): List<CdsWithNumaResource> {
        return cdsList.mapNotNull { (cdsId, numa) ->
            responses["$cdsId:$numa"]?.let { resList ->
                val cdsWithNumaResource = buildCdsResource(cdsId, numa, resList, requireGpu, requireCpu, requireEncoder)
                if (cdsWithNumaResource == null) {
                    logger.warn("[CDS] cds resource is not enough, cds: $cdsId:$numa, gpu: $requireGpu, " +
                            "cpu: $requireCpu, encoder: $requireEncoder")
                    return@mapNotNull null
                }
                cdsWithNumaResource
            }
        }
    }

    private fun buildCdsResource(
        cdsId: String,
        numa: Int,
        responses: List<Set<ZSetOperations.TypedTuple<String>>>,
        requireGpu: Int,
        requireCpu: Int,
        requireEncoder: Int
    ): CdsWithNumaResource? {

        val gpuData = responses[0].associate { it.value!! to (it.score?.let { it1 -> ceil(it1).toInt() } ?: 0) }
        if (gpuData.none { it.value >= requireGpu }) return null

        val cpuData = if (requireCpu > 0) {
            responses[1].associate { it.value!! to (it.score?.let { it1 -> ceil(it1).toInt() } ?: 0) }
                .takeIf { hasSufficientCpu(it, requireCpu) } ?: return null
        } else emptyMap()

        val encoderData = if (requireEncoder > 0) {
            responses[2].associate { it.value!! to (it.score?.let { it1 -> ceil(it1).toInt() } ?: 0) }
                .takeIf { hasValidEncoder(it, gpuData, requireEncoder, requireGpu) } ?: return null
        } else emptyMap()

        return CdsWithNumaResource(cdsId, numa, gpuData.toMutableMap(), cpuData.toMutableMap(), encoderData.toMutableMap())
    }

    private fun hasSufficientCpu(cpus: Map<String, Int>, required: Int): Boolean {
        val cpuGroup = CpuGroup()
        cpus.forEach { (cpu, score) ->
            if (score >= DEFAULT_CPU_ALLOC) {
                cpuGroup.addCpu(cpu)
            }
        }

        return cpuGroup.maxAvailable() >= required
    }

    private fun hasValidEncoder(
        encoders: Map<String, Int>,
        gpus: Map<String, Int>,
        encoderRequired: Int,
        gpuRequirements: Int
    ): Boolean {
        return encoders.any { (encoderId, score) ->
            score >= encoderRequired && gpus[EncoderUtils.parseGpuId(encoderId)]?.let { it >= gpuRequirements } ?: false
        }
    }

    data class CdsWithNuma(
        val cdsId: String,
        val numaIdx: Int
    )

    data class CdsWithNumaResource(
        val cdsId: String,
        val numaIdx: Int,
        val gpus: MutableMap<String, Int> = mutableMapOf(),
        val cpus: MutableMap<String, Int> = mutableMapOf(),
        val encoders: MutableMap<String, Int> = mutableMapOf(),
        val allocGpus: List<String> = emptyList(),
        val allocEncoders: List<String> = emptyList(),
        val maxGpu: Int = 0,
        val maxEncoder: Int = 0
    ) {

        fun allocGpuForBalance(gpuNeed: Int, encoderNeed: Int): AllocationQuadruple {
            if (encoderNeed <= 0) {
                val (gpu, maxGpu) = allocGpuOnly(gpuNeed)
                return AllocationQuadruple(gpu, maxGpu, "", -1)
            }

            val idx = mutableMapOf<Double, MutableList<String>>()
            val encoderOfGpu = mutableMapOf<String, String>()
            var minValue = Double.MAX_VALUE

            encoders.forEach { (encoderId, free) ->
                val gpuId = EncoderUtils.parseGpuId(encoderId)
                val gpuFree = gpus[gpuId] ?: return@forEach

                if (free < encoderNeed || gpuFree < gpuNeed) return@forEach

                encoderOfGpu[encoderId] = gpuId

                var diffEncoder = (free - encoderNeed).toDouble()
                var diffGpu = (gpuFree - gpuNeed).toDouble()

                diffEncoder = if (diffEncoder == 0.0) 0.1 else diffEncoder
                diffGpu = if (diffGpu == 0.0) 0.1 else diffGpu

                val balance = when {
                    diffGpu > diffEncoder -> abs(diffGpu / diffEncoder - 1)
                    else -> abs(diffEncoder / diffGpu - 1)
                }

                val areaBalWeight = (free * gpuFree * 1000).toDouble() + balance
                idx.computeIfAbsent(areaBalWeight) { mutableListOf() }.add(encoderId)

                if (areaBalWeight < minValue) {
                    minValue = areaBalWeight
                }
            }

            if (idx.isEmpty()) {
                return AllocationQuadruple("", -1, "", -1)
            }

            val candidates = idx[minValue] ?: emptyList()
            val selectedEncoder = candidates.random()
            val selectedGpu = encoderOfGpu[selectedEncoder] ?: ""

            encoders[selectedEncoder] = encoders.getValue(selectedEncoder) - encoderNeed
            gpus[selectedGpu] = gpus.getValue(selectedGpu) - gpuNeed

            val maxGpuFree = gpus.values.maxOrNull() ?: 0
            val maxEncoderFree = encoders.values.maxOrNull() ?: 0

            return AllocationQuadruple(selectedGpu, maxGpuFree, selectedEncoder, maxEncoderFree)
        }

        private fun allocGpuOnly(gpuNeed: Int): Pair<String, Int> {
            val idx = mutableMapOf<Int, MutableList<String>>()
            var minValue = Int.MAX_VALUE

            gpus.forEach { (gpuId, free) ->
                if (free < gpuNeed) return@forEach

                idx.computeIfAbsent(free) { mutableListOf() }.add(gpuId)

                if (free < minValue) {
                    minValue = free
                }
            }

            if (idx.isEmpty() || !idx.containsKey(minValue)) {
                return Pair("", -1)
            }

            val candidates = idx[minValue]!!
            val selectedGpu = candidates.random()

            gpus[selectedGpu] = gpus.getValue(selectedGpu) - gpuNeed

            val maxGpuFree = gpus.values.maxOrNull() ?: 0
            return Pair(selectedGpu, maxGpuFree)
        }

        fun allocCpu(cpuNeed: Int, cpuCoresNeed: Int): Pair<List<String>, Int> {
            val cpuGroup = CpuGroup().apply {
                cpus.filterValues { it >= cpuNeed }.keys.forEach { addCpu(it) }
            }
            return cpuGroup.selectOptimalCores(cpuCoresNeed) to cpuGroup.maxAvailable()
        }
    }

    private fun setUser(
        context: ScheduleContext,
        cdsInfoRecord: TCdsInfoRecord?,
        numaIdx: Int
    ): Result<Unit> = runCatching {
        val gameConfig = context.gameConfig

        val sessionInstance = SessionInstance(
            resourceAlloc = ResourceAllocation(
                cdsId = cdsInfoRecord?.cdsId ?: "",
                numa = numaIdx,
                useGpu = gameConfig?.resource?.gpu ?: 0,
                useCpuCores = gameConfig?.resource?.cpu ?: 0,
                useEncoder = gameConfig?.resource?.encoder ?: 0
            )
        )

        context.sessionInstance = sessionInstance
    }

    private fun getResPoolId(context: ScheduleContext): String {
        if (context.req.pkgId.isEmpty()) return ""
        return ""
    }

    private fun addCommandsToPipeline(
        context: ScheduleContext,
        cdsId: String,
        numa: Int,
        selectedGpuIds: List<String>,
        cpuList: List<String>,
        selectedEncoderIds: List<String>,
        maxGpuValue: Int,
        coresLeft: Int,
        maxEncoder: Int
    ) {
        val req = context.req
        val region = req.zoneId + if (context.isForPresentation) CommonRedisKey.RedisKeyOfPresentationSuffix else ""
        val gameConfig = context.gameConfig

        // 更新gpu资源
        selectedGpuIds.forEach {
            redisOperation.zincrBy(
                CommonRedisKey.redisKeyOfCgsGpuByCdsId(cdsId, numa),
                it,
                (-gameConfig?.resource?.gpu!!).toDouble()
            )
        }

        // 更新Encoder资源
        selectedEncoderIds.forEach { encoderId ->
            redisOperation.zincrBy(
                CommonRedisKey.redisKeyOfCgsEncoderByCdsId(cdsId, numa),
                encoderId,
                (-gameConfig?.resource?.encoder!!).toDouble()
            )
        }

        // 更新CPU资源
        val tuples = cpuList.map { cpu ->
            DefaultTypedTuple(cpu, 0.0)
        }
        if (cpuList.isNotEmpty()) {
            redisOperation.zaddTuples(
                CommonRedisKey.redisKeyOfCgsCPUByCdsId(cdsId, numa),
                tuples.toSet()
            )
        }

        // 预分配用户记录
        val uniqueUserId = req.userId.takeIf { it.isNotEmpty() }
            ?.let { "${it}|${req.clientType}" } ?: req.instanceId
        redisOperation.zadd(
            CommonRedisKey.redisKeyOfCgsUserPreAllocByCdsId(cdsId, numa),
            uniqueUserId,
            Instant.now().epochSecond.toDouble()
        )

        // 更新最大值统计
        val subKey = CommonRedisKey.redisSubKeyOfCdsIdNuma(cdsId, numa)
        listOf(
            Triple(CommonRedisKey.redisKeyOfRegionGpuMaxStaticByCdsId(region), maxGpuValue, subKey),
            Triple(CommonRedisKey.redisKeyOfRegionEncoderMaxStaticByCdsId(region), maxEncoder, subKey),
            Triple(CommonRedisKey.redisKeyOfRegionCPUCoresStaticByCdsId(region), coresLeft, subKey)
        ).forEach { (key, value, member) ->
            if (value != -1) {
                redisOperation.zadd(key, member, value.toDouble())
            }
        }
    }

    suspend fun calculateEmptyGpu(cdsId: String, region: String) {
        runCatching {
            val capacity = redisOperation.get(CommonRedisKey.redisKeyOfCgsGpuCapacityByCdsId(cdsId))?.toIntOrNull()
                ?: CommonRedisKey.DEFAULT_GPU_CAPACITY
            val (emptyNum, maxEmpty) = queryAndCalculateEmptyGpu(cdsId, capacity)
            val brand = redisOperation.get(CommonRedisKey.redisKeyOfCgsGpuBrandByCdsId(cdsId)) ?: CommonRedisKey.NVIDIA_BRAND
            addGpuEmptyStatistics(cdsId, brand, region, emptyNum, maxEmpty)

        }.onFailure { e ->
            logger.error("Failed to calculate empty GPU", e)
        }
    }

    private suspend fun queryAndCalculateEmptyGpu(cgsIp: String, capacity: Int): Pair<Int, Int> {
        val numaList = parseCgsNumaList(listOf(cgsIp))
        val minValue = (capacity - 1).toString()

        val countResults = mutableListOf<Long>()
        numaList.forEach { (cdsId, numa) ->
            countResults.add(
                redisOperation.zcount(
                    CommonRedisKey.redisKeyOfCgsGpuByCdsId(cdsId, numa),
                    minValue.toDouble(),
                    Double.MAX_VALUE
                ) ?: 0L
            )
        }

        val emptyCounter = AtomicInteger(0)
        val maxEmpty = AtomicInteger(0)
        countResults.forEach { count ->
            val current = count.toInt()
            emptyCounter.addAndGet(current)
            maxEmpty.updateAndGet { maxOf(it, current) }
        }

        return emptyCounter.get() to maxEmpty.get()
    }

    private suspend fun addGpuEmptyStatistics(
        cgsIp: String,
        brand: String,
        region: String,
        emptyNum: Int,
        maxEmptyNum: Int
    ) {
        val (emptyKey, maxKey) = when (brand) {
            CommonRedisKey.NVIDIA_BRAND ->
                CommonRedisKey.redisKeyOfCGSLeftEmptyGPUNByCdsId(region) to
                        CommonRedisKey.redisKeyOfCGSEachNumaMaxLeftEmptyGPUNByCdsId(region)

            else -> CommonRedisKey.redisKeyOfCGSLeftEmptyGPUAByCdsId(region) to
                    CommonRedisKey.redisKeyOfCGSEachNumaMaxLeftEmptyGPUAByCdsId(region)
        }

        redisOperation.zadd(emptyKey, cgsIp, emptyNum.toDouble())
        redisOperation.zadd(maxKey, cgsIp, maxEmptyNum.toDouble())
    }

    private fun parseCgsNumaList(ips: List<Any>): List<Pair<String, Int>> {
        // Implementation of IP-NUMA parsing logic
        return ips.map { ip ->
            val parts = ip.toString().split("_")
            parts[0] to parts.getOrElse(1) { "0" }.toInt()
        }
    }

    data class AllocationQuadruple(
        val selectedGpu: String,
        val maxGpuFree: Int,
        val selectedEncoder: String,
        val maxEncoderFree: Int
    ) {
        companion object {
            val EMPTY = AllocationQuadruple("", -1, "", -1)
        }
    }

    companion object {
        private const val DEFAULT_CPU_ALLOC = 100
        private val logger = LoggerFactory.getLogger(CdsAllocator::class.java)
        private const val CDS_REDIS_LOCK_PREFIX = "CDS_LOCK_"
    }
}