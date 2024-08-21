package com.tencent.devops.dispatch.kubernetes.pojo

data class TaskCallbackInfo(
    val taskUid: String,
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
