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
import com.tencent.devops.dispatch.kubernetes.pojo.BkMonitorRequestBody
import com.tencent.devops.dispatch.kubernetes.pojo.BkMonitorRequestBodyQueryConfigs
import com.tencent.devops.dispatch.kubernetes.pojo.BkMonitorResp
import com.tencent.devops.dispatch.kubernetes.pojo.BkMonitorRespData
import com.tencent.devops.dispatch.kubernetes.pojo.BkMonitorRespDataSeries
import okhttp3.MediaType.Companion.toMediaTypeOrNull
import okhttp3.Request
import okhttp3.RequestBody.Companion.toRequestBody
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Service
import java.time.Instant
import java.time.format.DateTimeFormatter

@Service
class BkMonitorMetricsService @Autowired constructor(
    private val client: Client,
    private val objectMapper: ObjectMapper
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

    fun queryMemoryUsageMetrics(
        userId: String,
        projectId: String,
        podName: String,
        startTime: Long,
        endTime: Long
    ): Map<String, List<Map<String, Any>>> {
        val promql = "sum(bkmonitor:container_memory_rss{bcs_cluster_id=\"BCS-K8S-26680\",namespace=\"manager-base\"," +
                "pod_name=\"$podName\"})"

        val data = searchMetrics(userId, projectId, promql, startTime, endTime)?.firstOrNull()?.datapoints

        val resultData = mutableMapOf<String, List<Map<String, Any>>>()
        val res = data?.map { d ->
            mapOf(
                "used_percent" to (d.getOrNull(0) ?: 0),
                "time" to if (d.getOrNull(1) == null) {
                    ""
                } else {
                    DateTimeFormatter.ISO_INSTANT.format(Instant.ofEpochMilli(d[1].toLong()))
                }
            )
        }
        resultData["used_percent"] = if (res.isNullOrEmpty()) {
            logger.warn("$userId|$projectId|$podName mem metrics is empty")
            listOf()
        } else {
            res
        }

        return resultData
    }

    fun queryCpuUsageMetrics(
        userId: String,
        projectId: String,
        podName: String,
        startTime: Long,
        endTime: Long
    ): Map<String, List<Map<String, Any>>> {
        val promql = "sum(rate(bkmonitor:container_cpu_usage_seconds_total{bcs_cluster_id=\"BCS-K8S-26680\"," +
                "namespace=\"manager-base\",pod_name=\"$podName\"}[2m]))"

        val data = searchMetrics(userId, projectId, promql, startTime, endTime)?.firstOrNull()?.datapoints

        val resultData = mutableMapOf<String, List<Map<String, Any>>>()
        val res = data?.map { d ->
            mapOf(
                "usage_user" to (d.getOrNull(0) ?: 0),
                "time" to if (d.getOrNull(1) == null) {
                    ""
                } else {
                    DateTimeFormatter.ISO_INSTANT.format(Instant.ofEpochMilli(d[1].toLong()))
                }
            )
        }
        resultData["usage_user"] = if (res.isNullOrEmpty()) {
            logger.warn("$userId|$projectId|$podName| cpu metrics is empty")
            listOf()
        } else {
            res
        }

        return resultData
    }

/*    fun queryDiskioMetrics(
        userId: String,
        projectId: String,
        agentHashId: String,
        os: String,
        timeRange: String
    ): Map<String, List<Map<String, Any>>> {
        val groupByTime: String = when (timeRange) {
            TIME_RANGE_WEEK -> "10m"
            TIME_RANGE_DAY -> "2m"
            else -> "10s"
        }
        val tag = when (OS.valueOf(os)) {
            OS.MACOS, OS.LINUX -> "name"
            OS.WINDOWS -> "instance"
        }
//        val (readPromql, writePromql) = when (OS.valueOf(agentRecord.os)) {
//            OS.MACOS, OS.LINUX -> Pair(
//                "abs(avg(rate($dataTableName:io:rkb_s{agentId=\"$agentId\"," +
//                    "projectId=\"$projectId\"}[$groupByTime])) by ($tag))",
//                "abs(avg(rate($dataTableName:io:wkb_s{agentId=\"$agentId\"," +
//                    "projectId=\"$projectId\"}[$groupByTime])) by ($tag))"
//            )
//
//            OS.WINDOWS -> Pair(
//                "avg($dataTableName:io:rkb_s{agentId=\"$agentId\",projectId=\"$projectId\"}) by ($tag)",
//                "avg($dataTableName:io:wkb_s{agentId=\"$agentId\",projectId=\"$projectId\"}) by ($tag)"
//            )
//
//            else -> return emptyMap()
//        }
        val readPromql = "abs(avg(rate($dataTableName:io:rkb_s{agentId=\"$agentHashId\"," +
                "projectId=\"$projectId\"}[$groupByTime])) by ($tag))"
        val writePromql = "abs(avg(rate($dataTableName:io:wkb_s{agentId=\"$agentHashId\"," +
                "projectId=\"$projectId\"}[$groupByTime])) by ($tag))"

        val readData = searchMetrics(projectId, readPromql, timeRange)
        val writeData = searchMetrics(projectId, writePromql, timeRange)

        val result = mutableMapOf<String, List<Map<String, Any>>>()
        result.putAll(formatData(tag, "read", readData))
        result.putAll(formatData(tag, "write", writeData))
        return result
    }

    fun queryNetMetrics(
        userId: String,
        projectId: String,
        agentHashId: String,
        os: String,
        timeRange: String
    ): Map<String, List<Map<String, Any>>> {
        val groupByTime: String = when (timeRange) {
            TIME_RANGE_WEEK -> "10m"
            TIME_RANGE_DAY -> "2m"
            else -> "10s"
        }
        val tag = when (OS.valueOf(os)) {
            OS.MACOS, OS.LINUX -> "interface"
            OS.WINDOWS -> "instance"
        }
        val readPromql = "abs(avg(rate($dataTableName:net:speed_recv{agentId=\"$agentHashId\"," +
                "projectId=\"$projectId\"}[$groupByTime])) by ($tag))"
        val sendPromql = "abs(avg(rate($dataTableName:net:speed_sent{agentId=\"$agentHashId\"," +
                "projectId=\"$projectId\"}[$groupByTime])) by ($tag))"

        val readData = searchMetrics(projectId, readPromql, timeRange)
        val sendData = searchMetrics(projectId, sendPromql, timeRange)

        val result = mutableMapOf<String, List<Map<String, Any>>>()
        result.putAll(formatData(tag, "IN", readData))
        result.putAll(formatData(tag, "OUT", sendData))
        return result
    }*/

    private fun formatData(
        tag: String,
        label: String,
        series: List<BkMonitorRespDataSeries>?
    ): Map<String, List<Map<String, Any>>> {
        if (series.isNullOrEmpty()) {
            return emptyMap()
        }

        val result = mutableMapOf<String, List<Map<String, Any>>>()
        series.forEach { s ->
            val dimension = s.dimensions?.get(tag) ?: return@forEach
            val fullLabel = "$dimension:$label"
            val data = s.datapoints?.map {
                parseDatapointItem(fullLabel, it)
            } ?: listOf()
            result[fullLabel] = data
        }

        return result
    }

    private fun parseDatapointItem(label: String, item: List<Double>): Map<String, Any> {
        // 前端计算需要数据*10与开源版influxdb相同
        val data = if (item.getOrNull(0) == null) {
            0
        } else {
            item[0] * 10
        }
        return mapOf(
            label to data,
            "time" to if (item.getOrNull(1) == null) {
                ""
            } else {
                DateTimeFormatter.ISO_INSTANT.format(Instant.ofEpochMilli(item[1].toLong()))
            }
        )
    }

    private fun searchMetrics(
        userId: String,
        projectId: String,
        promql: String,
        startTime: Long,
        endTime: Long
    ): List<BkMonitorRespDataSeries>? {
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
            startTime = 1722390808,
            endTime = 1722394408,
            slimit = 500,
            downSampleRange = "2s"
        )

        val data = requestBkMonitor(body)?.series

        logger.info("searchMetrics ${JsonUtil.toJson(body)} cost ${System.currentTimeMillis() - methodStartTime}ms, data: $data")

        return data
    }

    private fun getBizId(userId: String, projectId: String): Long? {
        return client.get(ServiceMonitorSpaceResource::class).getMonitorSpaceBizId(userId, projectId).data?.toLong()
    }

    private fun requestBkMonitor(body: Any): BkMonitorRespData? {
        return try {
            doRequestBkMonitor(body)
        } catch (e: Exception) {
            logger.warn("requestBkMonitor error", e)
            null
        }
    }

    private fun doRequestBkMonitor(body: Any): BkMonitorRespData? {
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
    }
}
