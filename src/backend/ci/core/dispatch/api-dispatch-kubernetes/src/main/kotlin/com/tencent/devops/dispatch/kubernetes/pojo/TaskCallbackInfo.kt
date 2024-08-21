package com.tencent.devops.dispatch.kubernetes.pojo

data class TaskCallbackInfo(
    val taskId: String,
    val podName: String,
    val status: CallbackTaskStatus
)

enum class CallbackTaskStatus {
    waiting,
    running,
    succeeded,
    failed,
    abort
}
