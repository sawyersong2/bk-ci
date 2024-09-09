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

package com.tencent.devops.dispatch.kubernetes.dao

import com.tencent.devops.model.dispatch.kubernetes.tables.TDispatchKubernetesJobHistory
import com.tencent.devops.model.dispatch.kubernetes.tables.records.TDispatchKubernetesJobHistoryRecord
import org.jooq.DSLContext
import org.jooq.Result
import org.springframework.stereotype.Repository

@Repository
class DispatchKubernetesJobHisDao {

    fun createJobHistory(
        dslContext: DSLContext,
        projectId: String,
        pipelineId: String,
        buildId: String,
        vmSeqId: String,
        taskId: String,
        executeCount: Int,
        jobName: String,
        jobTag: String,
        cpu: Double,
        memory: Double,
        disk: String
    ): Long {
        with(TDispatchKubernetesJobHistory.T_DISPATCH_KUBERNETES_JOB_HISTORY) {
            val preRecord = dslContext.selectFrom(this)
                .where(BUILD_ID.eq(buildId))
                .and(VM_SEQ_ID.eq(vmSeqId))
                .and(TASK_ID.eq(taskId))
                .and(EXECUTE_COUNT.eq(executeCount))
                .and(JOB_TAG.eq(jobTag))
                .fetch()
            if (preRecord.size > 0) {
                dslContext.deleteFrom(this)
                    .where(BUILD_ID.eq(buildId))
                    .and(VM_SEQ_ID.eq(vmSeqId))
                    .and(TASK_ID.eq(taskId))
                    .and(EXECUTE_COUNT.eq(executeCount))
                    .and(JOB_TAG.eq(jobTag))
                    .execute()
            }

            return dslContext.insertInto(
                this,
                PROJECT_ID,
                PIPELINE_ID,
                BUILD_ID,
                VM_SEQ_ID,
                TASK_ID,
                EXECUTE_COUNT,
                JOB_NAME,
                JOB_TAG,
                CPU,
                MEMORY,
                DISK
            ).values(
                projectId,
                pipelineId,
                buildId,
                vmSeqId,
                taskId,
                executeCount,
                jobName,
                jobTag,
                cpu,
                memory,
                disk
            ).returning(ID).fetchOne()?.id ?: 1
        }
    }

    fun getPipelineJobHistory(
        dslContext: DSLContext,
        pipelineId: String,
        vmSeqId: String,
        taskId: String,
        executeCount: Int,
        jobTag: String
    ): Result<TDispatchKubernetesJobHistoryRecord> {
        with(TDispatchKubernetesJobHistory.T_DISPATCH_KUBERNETES_JOB_HISTORY) {
            return dslContext.selectFrom(this)
                .where(PIPELINE_ID.eq(pipelineId))
                .and(VM_SEQ_ID.eq(vmSeqId))
                .and(TASK_ID.eq(taskId))
                .and(EXECUTE_COUNT.eq(executeCount))
                .and(JOB_TAG.eq(jobTag))
                .fetch()
        }
    }

    fun getBuildJobHistory(
        dslContext: DSLContext,
        buildId: String,
        vmSeqId: String,
        executeCount: Int
    ): Result<TDispatchKubernetesJobHistoryRecord> {
        with(TDispatchKubernetesJobHistory.T_DISPATCH_KUBERNETES_JOB_HISTORY) {
            return dslContext.selectFrom(this)
                .where(BUILD_ID.eq(buildId))
                .and(VM_SEQ_ID.eq(vmSeqId))
                .and(EXECUTE_COUNT.eq(executeCount))
                .fetch()
        }
    }

    fun updateWorkloadName(
        dslContext: DSLContext,
        buildId: String,
        vmSeqId: String,
        taskId: String,
        jobTag: String,
        executeCount: Int,
        podName: String,
        clusterId: String,
        namespace: String
    ) {
        with(TDispatchKubernetesJobHistory.T_DISPATCH_KUBERNETES_JOB_HISTORY) {
            dslContext.update(this)
                .set(POD_NAME, podName)
                .set(CLUSTER_ID, clusterId)
                .set(NAMESPACE, namespace)
                .where(BUILD_ID.eq(buildId))
                .and(VM_SEQ_ID.eq(vmSeqId))
                .and(TASK_ID.eq(taskId))
                .and(JOB_TAG.eq(jobTag))
                .and(EXECUTE_COUNT.eq(executeCount))
                .execute()
        }
    }

    fun updateWorkloadUsage(
        dslContext: DSLContext,
        id: Long,
        cpuPercentile: Double,
        memPercentile: Double,
        cpuMetrics: String,
        memMetrics: String
    ) {
        with(TDispatchKubernetesJobHistory.T_DISPATCH_KUBERNETES_JOB_HISTORY) {
            dslContext.update(this)
                .set(REAL_CPU_PERCENTILE, cpuPercentile)
                .set(REAL_MEM_PERCENTILE, memPercentile)
                .set(REAL_CPU_METRICS, cpuMetrics)
                .set(REAL_MEM_METRICS, memMetrics)
                .where(ID.eq(id))
                .execute()
        }
    }
}
