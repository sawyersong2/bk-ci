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

import com.tencent.devops.common.api.util.JsonUtil
import com.tencent.devops.common.web.utils.I18nUtil
import com.tencent.devops.dispatch.kubernetes.pojo.BK_KUBERNETES_TASK_EXECUTE_TIMEOUT
import com.tencent.devops.dispatch.kubernetes.pojo.TaskCallbackAction
import com.tencent.devops.dispatch.kubernetes.pojo.TaskCallbackInfo
import com.tencent.devops.dispatch.kubernetes.pojo.TaskCallbackStatus
import com.tencent.devops.dispatch.kubernetes.pojo.base.DispatchBuildStatusResp
import com.tencent.devops.dispatch.kubernetes.utils.TaskCallbackRedisUtils
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service

@Service
class DispatchBaseTaskService @Autowired constructor(
    private val taskCallbackRedisUtils: TaskCallbackRedisUtils
) {

    fun getTaskStatus(
        userId: String,
        projectId: String,
        buildId: String,
        taskId: String
    ): DispatchBuildStatusResp {
        // 从redis中获取任务状态,只要状态存在说明已回调成功
        val taskCallbackInfo = taskCallbackRedisUtils.getTaskCallbackInfo(taskId)
        if (taskCallbackInfo?.status != null) {
            return DispatchBuildStatusResp(
                status = taskCallbackInfo.status.name,
                errorMsg = taskCallbackInfo.message
            )
        }
        return DispatchBuildStatusResp(
            status = TaskCallbackStatus.running.name,
            errorMsg = ""
        )
    }

    fun taskCallback(taskCallbackInfo: TaskCallbackInfo): Boolean {
        logger.info("TaskCallback: ${JsonUtil.toJson(taskCallbackInfo)}")
        taskCallbackRedisUtils.refreshTaskCallbackInfo(taskCallbackInfo)
        return true
    }

    fun waitTaskFinish(userId: String, taskId: String): TaskCallbackInfo {
        val startTime = System.currentTimeMillis()
        loop@ while (true) {
            if (System.currentTimeMillis() - startTime > WAIT_TASK_TIMEOUT) {
                logger.error("$taskId kubernetes task timeout")
                return TaskCallbackInfo(
                    status = TaskCallbackStatus.timeout,
                    podName = "",
                    namespace = "",
                    message = "${I18nUtil.getCodeLanMessage(BK_KUBERNETES_TASK_EXECUTE_TIMEOUT)}（10min）",
                    action = TaskCallbackAction.unkown,
                    taskId = taskId
                )
            }
            Thread.sleep(1 * 1000)

            // 从redis中获取任务状态,只要状态存在说明已回调成功
            val taskCallbackInfo = taskCallbackRedisUtils.getTaskCallbackInfo(taskId)
            if (taskCallbackInfo?.status != null) {
                logger.info("Loop task taskId: $taskId, status: ${JsonUtil.toJson(taskCallbackInfo)}")
                return taskCallbackInfo
            }
        }
    }

    companion object {
        private val logger = LoggerFactory.getLogger(DispatchBaseTaskService::class.java)
        private const val WAIT_TASK_TIMEOUT = 10 * 60 * 1000
    }
}
