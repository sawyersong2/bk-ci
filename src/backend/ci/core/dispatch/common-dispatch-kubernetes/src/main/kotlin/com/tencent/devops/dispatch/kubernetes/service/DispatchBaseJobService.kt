/*
 * Tencent is pleased to support the open source community by making BK-CI 蓝鲸持续集成平台 available.
 *
 * Copyright (C) 2019 Tencent.  All rights reserved.
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

import com.tencent.devops.dispatch.kubernetes.dao.DispatchKubernetesJobHisDao
import com.tencent.devops.dispatch.kubernetes.pojo.TaskCallbackStatus
import com.tencent.devops.dispatch.kubernetes.pojo.base.DispatchBuildStatusResp
import com.tencent.devops.dispatch.kubernetes.pojo.base.DispatchJobLogResp
import com.tencent.devops.dispatch.kubernetes.pojo.base.DispatchJobReq
import com.tencent.devops.dispatch.kubernetes.pojo.base.DispatchTaskResp
import com.tencent.devops.dispatch.kubernetes.service.factory.JobServiceFactory
import com.tencent.devops.dispatch.kubernetes.utils.ThreadPoolName
import com.tencent.devops.dispatch.kubernetes.utils.ThreadPoolUtils
import org.jooq.DSLContext
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Service

@Service
class DispatchBaseJobService @Autowired constructor(
    private val dslContext: DSLContext,
    private val jobServiceFactory: JobServiceFactory,
    private val dispatchBaseTaskService: DispatchBaseTaskService,
    private val dispatchKubernetesJobHisDao: DispatchKubernetesJobHisDao
) {

    companion object {
        private val logger = LoggerFactory.getLogger(DispatchBaseJobService::class.java)
        private const val MAX_LIMIT_CPU = 32.0
        private const val MAX_LIMIT_MEMORY = 65535
        private const val MAX_LIMIT_DISK = 100
    }

    @Value("\${kubernetes.resources.job.cpu}")
    var cpu: Double = 32.0

    @Value("\${kubernetes.resources.job.memory}")
    var memory: Int = 65535

    @Value("\${kubernetes.resources.job.disk}")
    var disk: Int = 500

    @Value("\${kubernetes.clusterId:}")
    val kubernetesClusterId: String = ""

    fun createJob(
        userId: String,
        projectId: String,
        pipelineId: String,
        buildId: String,
        vmSeqId: String,
        taskId: String,
        executeCount: Int,
        jobReq: DispatchJobReq
    ): DispatchTaskResp {
        val logPrefix = "$userId|$projectId|$pipelineId|$buildId|$vmSeqId|$executeCount|$taskId|${jobReq.jobTag}"
        logger.info("$logPrefix createJob: $jobReq")
        val limitCpu = jobReq.limitCpu?.coerceAtMost(MAX_LIMIT_CPU) ?: cpu
        val limitMemory = jobReq.limitMemory?.coerceAtMost(MAX_LIMIT_MEMORY) ?: memory
        val limitDisk = jobReq.limitDisk?.coerceAtMost(MAX_LIMIT_DISK) ?: disk

        jobReq.limitCpu = limitCpu
        jobReq.limitMemory = limitMemory
        jobReq.limitDisk = limitDisk

        dispatchKubernetesJobHisDao.createJobHistory(
            dslContext = dslContext,
            projectId = projectId,
            pipelineId = pipelineId,
            buildId = buildId,
            vmSeqId = vmSeqId,
            executeCount = executeCount,
            taskId = taskId,
            jobTag = jobReq.jobTag ?: jobReq.image.substringBefore(":"),
            jobName = jobReq.alias,
            cpu = limitCpu,
            memory = limitMemory.toDouble(),
            disk = limitDisk.toString()
        )

        val jobResp = jobServiceFactory.load(projectId).createJob(userId, jobReq)
        if (jobResp.taskId.isEmpty()) {
            logger.error("$logPrefix createJob failed. ${jobResp.errorMsg}")
            return DispatchTaskResp(
                taskId = "",
                taskStatus = TaskCallbackStatus.failed,
                errorMsg = jobResp.errorMsg
            )
        }

        ThreadPoolUtils.getInstance().getThreadPool(ThreadPoolName.WAIT_TASK_FINISH.name).execute {
            val taskCallbackInfo = dispatchBaseTaskService.waitTaskFinish(userId, jobResp.taskId)
            // 根据回调信息，记录负载集群和域名等信息
            dispatchKubernetesJobHisDao.updateWorkloadName(
                dslContext = dslContext,
                buildId = buildId,
                vmSeqId = vmSeqId,
                executeCount = executeCount,
                taskId = taskId,
                jobTag = jobReq.jobTag ?: "",
                podName = taskCallbackInfo.podName,
                clusterId = kubernetesClusterId,
                namespace = taskCallbackInfo.namespace
            )
        }

        return DispatchTaskResp(
            taskId = jobResp.taskId,
            taskStatus = TaskCallbackStatus.running
        )
    }

    fun getJobStatus(
        userId: String,
        projectId: String,
        buildId: String,
        jobName: String
    ): DispatchBuildStatusResp {
        return jobServiceFactory.load(projectId).getJobStatus(userId, jobName)
    }

    fun getJobLogs(
        userId: String,
        projectId: String,
        buildId: String,
        jobName: String,
        sinceTime: Int?
    ): DispatchJobLogResp {
        return jobServiceFactory.load(projectId).getJobLogs(userId, jobName, sinceTime)
    }
}
