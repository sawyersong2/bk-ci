/*
 * Tencent is pleased to support the open source community by making BK-CI 蓝鲸持续集成平台 available.
 *
 * Copyright (C) 2019 THL A29 Limited, a Tencent company.  All rights reserved.
 *
 * BK-CI 蓝鲸持续集成平台 is licensed under the MIT license.
 *
 * A copy of the MIT License is included in this file.
 *
 *
 * Terms of the MIT License:
 * ---------------------------------------------------
 * Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated
 * documentation files (the "Software"), to deal in the Software without restriction, including without limitation the
 * rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all copies or substantial portions of
 * the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT
 * LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN
 * NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
 * WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
 * SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */

package com.tencent.devops.dispatch.kubernetes.service

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import com.tencent.devops.auth.api.service.ServiceMonitorSpaceResource
import com.tencent.devops.common.api.exception.RemoteServiceException
import com.tencent.devops.common.api.util.JsonUtil
import com.tencent.devops.common.api.util.OkhttpUtils
import com.tencent.devops.common.client.Client
import com.tencent.devops.common.dispatch.sdk.pojo.docker.DockerRoutingType
import com.tencent.devops.dispatch.kubernetes.dao.DispatchKubernetesBuildHisDao
import com.tencent.devops.dispatch.kubernetes.dao.DispatchKubernetesJobHisDao
import com.tencent.devops.dispatch.kubernetes.pojo.BkMonitorRequestBody
import com.tencent.devops.dispatch.kubernetes.pojo.BkMonitorRequestBodyQueryConfigs
import com.tencent.devops.dispatch.kubernetes.pojo.BkMonitorResp
import com.tencent.devops.dispatch.kubernetes.pojo.BkMonitorRespData
import com.tencent.devops.process.pojo.mq.PipelineAgentShutdownEvent
import okhttp3.MediaType.Companion.toMediaTypeOrNull
import okhttp3.Request
import okhttp3.RequestBody.Companion.toRequestBody
import org.jooq.DSLContext
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Service
import java.math.BigDecimal
import java.math.RoundingMode
import java.time.LocalDateTime
import java.time.ZoneOffset
import kotlin.streams.toList

@Service
class BkMonitorMetricsService @Autowired constructor(
    private val client: Client,
    private val objectMapper: ObjectMapper,
    private val dslContext: DSLContext,
    private val dispatchKubernetesBuildHisDao: DispatchKubernetesBuildHisDao,
    private val dispatchKubernetesJobHisDao: DispatchKubernetesJobHisDao
) {

    @Value("\${bkMonitor.gateway:#{null}}")
    val bkMonitorGateway: String? = null

    @Value("\${bkMonitor.appCode:#{null}}")
    val bkMonitorAppCode: String? = null

    @Value("\${bkMonitor.appSecret:#{null}}")
    val bkMonitorAppSecret: String? = null

    companion object {
        private val logger = LoggerFactory.getLogger(BkMonitorMetricsService::class.java)
    }

    fun calculateWorkloadUsage(dockerRoutingType: DockerRoutingType, event: PipelineAgentShutdownEvent) {
        val commonProcessor: (buildHistory: BuildHistory) -> Unit = { buildHistory ->
            val startTime = buildHistory.createTime.plusSeconds(10).toEpochSecond(ZoneOffset.of("+8"))
            val endTime = LocalDateTime.now().toEpochSecond(ZoneOffset.of("+8"))

            val cpuMetrics = queryUsageMetrics(buildHistory, startTime, endTime, MetricType.CPU)
            val cpuPercentile = cpuMetrics.percentile(80.0) ?: 0.0

            val memoryMetrics = queryUsageMetrics(buildHistory, startTime, endTime, MetricType.MEMORY)
            val memoryPercentile = memoryMetrics.percentile(80.0) ?: 0.0

            updateWorkloadUsage(buildHistory, cpuPercentile, cpuMetrics, memoryPercentile, memoryMetrics)
        }

        dispatchKubernetesBuildHisDao.get(
            dslContext = dslContext,
            buildId = event.buildId,
            vmSeqId = event.vmSeqId ?: "",
            dispatchType = dockerRoutingType.name
        ).first()?.let {
            BuildHistory(
                id = it.id,
                podName = it.podName,
                clusterId = it.clusterId,
                namespace = it.namespace,
                createTime = it.createTime,
                workloadType = WorkloadType.DEPLOYMENT
            ).let(commonProcessor)
        }

        // 如果当前构建内包含kubernetes job构建，则收集负载数据
        dispatchKubernetesJobHisDao.getBuildJobHistory(
            dslContext = dslContext,
            buildId = event.buildId,
            vmSeqId = event.vmSeqId ?: "",
            executeCount = event.executeCount ?: 1
        ).stream().map { BuildHistory(
            id = it.id,
            podName = it.podName,
            clusterId = it.clusterId,
            namespace = it.namespace,
            createTime = it.createdTime,
            workloadType = WorkloadType.JOB
        ) }.toList().forEach(commonProcessor)
    }

    private enum class MetricType {
        CPU, MEMORY
    }
    private enum class WorkloadType {
        DEPLOYMENT, JOB
    }

    private data class BuildHistory(
        val id: Long,
        val podName: String,
        val clusterId: String,
        val namespace: String,
        val createTime: LocalDateTime,
        val workloadType: WorkloadType
    )

    private fun queryUsageMetrics(
        buildHistory: BuildHistory,
        startTime: Long,
        endTime: Long,
        metricType: MetricType
    ): List<Double> {
        val promql = when (metricType) {
            MetricType.CPU -> "sum(rate(bkmonitor:container_cpu_usage_seconds_total" +
                    "{bcs_cluster_id=\"${buildHistory.clusterId}\",namespace=\"${buildHistory.namespace}\"," +
                    "pod_name=\"${buildHistory.podName}\"}[2m]))"
            MetricType.MEMORY -> "sum(bkmonitor:container_memory_rss" +
                    "{bcs_cluster_id=\"${buildHistory.clusterId}\",namespace=\"${buildHistory.namespace}\"," +
                    "pod_name=\"${buildHistory.podName}\"})"
        }

        val methodStartTime = System.currentTimeMillis()
        // val bizId = getBizId(userId, projectId) ?: return null
        val body = BkMonitorRequestBody(
            bkBizId = -4238528,
            queryConfigs = listOf(
                BkMonitorRequestBodyQueryConfigs(
                    alias = "a",
                    dataSourceLabel = "prometheus",
                    dataTypeLabel = "time_series",
                    promql = promql,
                    interval = 60
                )
            ),
            expression = "",
            startTime = startTime,
            endTime = endTime,
            slimit = 500,
            downSampleRange = "2s"
        )

        val dataPoints = requestBkMonitor(body)?.series?.firstOrNull()?.datapoints

        logger.info("searchMetrics ${JsonUtil.toJson(body)} cost ${System.currentTimeMillis() - methodStartTime}ms, " +
                "response: $dataPoints")

        return dataPoints?.mapNotNull { it.getOrNull(0) }?.toList() ?: emptyList()
    }

    private fun updateWorkloadUsage(
        buildHistory: BuildHistory,
        cpuPercentile: Double,
        cpuMetrics: List<Double>,
        memoryPercentile: Double,
        memoryMetrics: List<Double>
    ) {
        val cpuPercentileFormat = BigDecimal.valueOf(cpuPercentile)
            .setScale(2, RoundingMode.HALF_UP).toDouble()
        val memPercentileFormat = BigDecimal.valueOf(memoryPercentile).divide(BigDecimal.valueOf(1024 * 1024))
            .setScale(2, RoundingMode.HALF_UP).toDouble()
        when (buildHistory.workloadType) {
            WorkloadType.DEPLOYMENT -> {
                dispatchKubernetesBuildHisDao.updateWorkloadUsage(
                    dslContext = dslContext,
                    id = buildHistory.id,
                    cpuPercentile = cpuPercentileFormat,
                    cpuMetrics = cpuMetrics.toString(),
                    memPercentile = memPercentileFormat,
                    memMetrics = memoryMetrics.toString()
                )
            }
            WorkloadType.JOB -> {
                dispatchKubernetesJobHisDao.updateWorkloadUsage(
                    dslContext = dslContext,
                    id = buildHistory.id,
                    cpuPercentile = cpuPercentileFormat,
                    cpuMetrics = cpuMetrics.toString(),
                    memPercentile = memPercentileFormat,
                    memMetrics = memoryMetrics.toString()
                )
            }
        }
    }

    private fun getBizId(userId: String, projectId: String): Long? {
        return client.get(ServiceMonitorSpaceResource::class).getMonitorSpaceBizId(userId, projectId).data?.toLong()
    }

    private fun <T : Comparable<T>> List<T>.percentile(percentage: Double): Double? {
        if (this.isEmpty()) return null

        val sortedList = this.sorted()
        val size = sortedList.size
        val index = (percentage / 100) * (size - 1)
        val lowerIndex = index.toInt()
        val upperIndex = if (index == lowerIndex.toDouble()) lowerIndex else lowerIndex + 1

        return if (lowerIndex == upperIndex) {
            sortedList[lowerIndex] as Double
        } else {
            val lowerValue = sortedList[lowerIndex] as Double
            val upperValue = sortedList[upperIndex] as Double
            lowerValue + (index - lowerIndex) * (upperValue - lowerValue)
        }
    }

    private fun requestBkMonitor(body: Any): BkMonitorRespData? {
        return try {
            val url = "$bkMonitorGateway/time_series/unify_query"
            val headerStr = objectMapper.writeValueAsString(
                mapOf("bk_app_code" to bkMonitorAppCode, "bk_app_secret" to bkMonitorAppSecret)
            ).replace("\\s".toRegex(), "")
            val requestBody = objectMapper.writeValueAsString(body)
                .toRequestBody("application/json; charset=utf-8".toMediaTypeOrNull())

            val request = Request.Builder()
                .url(url)
                .post(requestBody)
                .addHeader("X-Bkapi-Authorization", headerStr)
                .build()

            OkhttpUtils.doHttp(request).use {
                if (!it.isSuccessful) {
                    logger.warn("request failed, uri:($url)|response: ($it)")
                    throw RemoteServiceException("request failed, response:($it)")
                }
                val responseStr = it.body!!.string()
                val resp = objectMapper.readValue<BkMonitorResp>(responseStr)
                if (resp.code != 200L || !resp.result) {
                    // 请求错误
                    logger.warn("request failed, url:($url)|response:($it)")
                    throw RemoteServiceException("request failed, response:(${resp.message})")
                }
                logger.debug("request response：${objectMapper.writeValueAsString(resp.data)}")
                return resp.data
            }
        } catch (e: Exception) {
            logger.warn("requestBkMonitor error", e)
            null
        }
    }
}
