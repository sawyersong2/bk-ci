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
package com.tencent.devops.dispatch.kubernetes.utils

import com.fasterxml.jackson.databind.ObjectMapper
import com.tencent.devops.common.api.util.JsonUtil
import com.tencent.devops.common.redis.RedisOperation
import com.tencent.devops.dispatch.kubernetes.pojo.TaskCallbackInfo
import java.util.concurrent.TimeUnit
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Component

@Component
class TaskCallbackRedisUtils @Autowired constructor(
    private val redisOperation: RedisOperation,
    private val objectMapper: ObjectMapper
) {

    /*-------------------------*/
    fun refreshTaskCallbackInfo(taskCallbackInfo: TaskCallbackInfo) {
        logger.info("RefreshTaskCallbackInfo hset(${taskCallbackKey()}:${taskCallbackInfo.taskId})")
        redisOperation.set(
            key = "${taskCallbackKey()}:${taskCallbackInfo.taskId}",
            value = JsonUtil.toJson(taskCallbackInfo),
            expiredInSecond = EXPIRED_SECOND
        )
    }

    fun getTaskCallbackInfo(taskId: String): TaskCallbackInfo? {
        val result = redisOperation.get("${taskCallbackKey()}:$taskId")
        logger.info("${taskCallbackKey()}:$taskId get task: $result")
        return if (result != null) {
            return objectMapper.readValue(result, TaskCallbackInfo::class.java)
        } else {
            null
        }
    }

    fun deleteTaskCallbackInfo(taskId: String) {
        logger.info("DeleteTaskCallbackInfo hdelete(${taskCallbackKey()}:$taskId)")
        redisOperation.delete("${taskCallbackKey()}:$taskId")
    }

    private fun taskCallbackKey(): String {
        return "dispatch:kubernetes:task_callback_info"
    }

    companion object {
        private val logger = LoggerFactory.getLogger(TaskCallbackRedisUtils::class.java)
        private val EXPIRED_SECOND = TimeUnit.DAYS.toSeconds(1)
    }
}
